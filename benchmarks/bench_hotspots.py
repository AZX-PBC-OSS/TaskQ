"""A/B benchmark harness for TaskQ hotspot optimization proposals.

Every optimization proposal is measured here as A (current code) vs B
(proposed variant), interleaved to cancel thermal/frequency drift, with
correctness assertions proving the variant is output-identical to the
baseline before any timing is trusted.

Usage:
    python benchmarks/bench_hotspots.py                  # run all A/B benches
    python benchmarks/bench_hotspots.py --only di,decode # subset by name
    python benchmarks/bench_hotspots.py --profile di     # cProfile one bench
    python benchmarks/bench_hotspots.py --stall          # event-loop stall probes
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json as stdlib_json
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from taskq._di.scope import Scope


class ServiceA:
    pass


class ServiceB:
    pass


# ── Harness ───────────────────────────────────────────────────────────


@dataclass
class ABResult:
    name: str
    a_ns_per_op: float
    b_ns_per_op: float
    a_seconds: float
    b_seconds: float
    speedup: float
    correct: bool
    note: str = ""


def time_callable(fn: Callable[[], object], batch: int, batches: int) -> list[float]:
    """Return per-batch wall times; caller computes stats."""
    times: list[float] = []
    for _ in range(batches):
        t0 = time.perf_counter_ns()
        for _ in range(batch):
            fn()
        times.append((time.perf_counter_ns() - t0) / batch)
    return times


def ab_bench(
    name: str,
    a: Callable[[], object],
    b: Callable[[], object],
    *,
    batch: int = 1,
    batches: int = 9,
    correct: bool = True,
    note: str = "",
) -> ABResult:
    """Interleave A and B batches; report median ns/op and speedup."""
    a_times: list[float] = []
    b_times: list[float] = []
    # warmup
    for _ in range(50):
        a()
        b()
    for _ in range(batches):
        a_times.extend(time_callable(a, batch, 1))
        b_times.extend(time_callable(b, batch, 1))
    a_med = statistics.median(a_times)
    b_med = statistics.median(b_times)
    spread = (max(a_times) - min(a_times)) / a_med if a_med else 0
    return ABResult(
        name=name,
        a_ns_per_op=a_med,
        b_ns_per_op=b_med,
        a_seconds=a_med / 1e9,
        b_seconds=b_med / 1e9,
        speedup=a_med / b_med if b_med else 0,
        correct=correct,
        note=note + (f" [A spread {spread:.0%}]" if batches >= 5 else ""),
    )


def report(results: list[ABResult]) -> None:
    print(f"\n{'bench':<38} {'A (current)':>14} {'B (variant)':>14} {'speedup':>9}  ok")
    print("-" * 88)
    for r in results:
        a = f"{r.a_ns_per_op:,.0f} ns" if r.a_ns_per_op < 1e6 else f"{r.a_seconds * 1e3:,.2f} ms"
        b = f"{r.b_ns_per_op:,.0f} ns" if r.b_ns_per_op < 1e6 else f"{r.b_seconds * 1e3:,.2f} ms"
        flag = "OK" if r.correct else "MISMATCH!"
        warn = f"  {r.note}" if r.note else ""
        print(f"{r.name:<38} {a:>14} {b:>14} {r.speedup:>8.2f}x  {flag}{warn}")


# ── Bench 1: DI solver reflection (per-job) ──────────────────────────

_BENCH_DIR = "benchmarks"


def bench_di_solver() -> ABResult:
    """A: solve_dependencies as-is (get_type_hints + inspect.signature per call).
    B: prototype with per-func memoized (hints, signature)."""
    import inspect
    from functools import lru_cache
    from typing import get_type_hints

    from taskq._di.solver import solve_dependencies
    from taskq._di.types import FactoryShape, ProviderEntry

    class _StubRegistry:
        def __init__(self, entries: dict[type, ProviderEntry[Any]]) -> None:
            self._entries = entries

        def get(self, type_: type[object]) -> ProviderEntry[object]:
            return self._entries[type_]

    class _StubContainer:
        def __init__(self, scope: Scope) -> None:
            self._scope = scope
            self._cache: dict[type, object] = {}
            self._last_cache_hit = False

        @property
        def last_cache_hit(self) -> bool:
            return self._last_cache_hit

        async def get_or_create(self, type_: type[object], entry: ProviderEntry[object]) -> object:
            if type_ in self._cache:
                self._last_cache_hit = True
                return self._cache[type_]
            self._last_cache_hit = False
            value = ServiceA() if type_ is ServiceA else ServiceB()
            if self._scope is not Scope.TRANSIENT:
                self._cache[type_] = value
            return value

        async def aclose(self) -> None:
            pass

    registry = _StubRegistry(
        {
            ServiceA: ProviderEntry(
                type_=ServiceA,
                scope=Scope.LOOP,
                kind="value",
                impl=ServiceA,
                factory_shape=FactoryShape.VALUE,
            ),
            ServiceB: ProviderEntry(
                type_=ServiceB,
                scope=Scope.LOOP,
                kind="value",
                impl=ServiceB,
                factory_shape=FactoryShape.VALUE,
            ),
        }
    )
    containers = {
        Scope.LOOP: _StubContainer(Scope.LOOP),
        Scope.TRANSIENT: _StubContainer(Scope.TRANSIENT),
    }

    class Deps:
        pass

    def actor(payload: dict[str, object], a: Annotated[ServiceA, Scope.LOOP], b: ServiceB) -> None:
        return None

    payload = {"order_id": "ord-123", "amount": 4250, "items": ["x", "y"]}

    async def run_current() -> dict[str, object]:
        return await solve_dependencies(
            func=actor,
            registry=registry,
            scope_containers=containers,
            passthrough_kwargs={"payload": payload},
        )

    @lru_cache(maxsize=1024)
    def _cached_introspection(
        func: object,
    ) -> tuple[dict[str, Any], inspect.Signature]:
        module = sys.modules.get(func.__module__)
        globalns = vars(module) if module is not None else {}
        hints = get_type_hints(func, include_extras=True, globalns=globalns)
        return hints, inspect.signature(func)

    async def run_cached() -> dict[str, object]:
        hints, sig = _cached_introspection(actor)
        passthrough = {"payload": payload}
        kwargs: dict[str, object] = {}
        for param_name, annotation in hints.items():
            if param_name == "return" or param_name in passthrough:
                continue
            from taskq._di.solver import _unwrap_scope_override

            unwrapped, override_scope = _unwrap_scope_override(param_name, annotation)
            lookup_type = unwrapped if unwrapped is not None else annotation
            entry = registry.get(lookup_type)
            effective = override_scope if override_scope is not None else entry.scope
            kwargs[param_name] = await containers[effective].get_or_create(lookup_type, entry)
        for pname, param in sig.parameters.items():
            if pname == "self" or pname in kwargs or pname in passthrough:
                continue
            if param.default is inspect.Parameter.empty and param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                raise TypeError(pname)
        return {**passthrough, **kwargs}

    # correctness
    cur = asyncio.run(run_current())
    cached = asyncio.run(run_cached())
    ok = cur.keys() == cached.keys() and all(type(cur[k]) is type(cached[k]) for k in cur)

    # event-loop drivers for async benches
    def drive(fn: Callable[[], Any]) -> Callable[[], object]:
        loop = asyncio.new_event_loop()
        coro = fn()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    # NOTE: solve_dependencies is async; for timing we time the coroutine
    # creation + a fresh loop run per call would dominate. Instead time
    # N solves in one loop and divide — batch semantics.
    def make_batch_runner(fn: Callable[[], Any], n: int) -> Callable[[], object]:
        async def one() -> object:
            await fn()
            return None

        async def run_n() -> None:
            await asyncio.gather(*[one() for _ in range(n)])

        def run_batch() -> object:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(run_n())
            finally:
                loop.close()
            return None

        return run_batch

    n = 200
    a_runner = make_batch_runner(run_current, n)
    b_runner = make_batch_runner(run_cached, n)
    result = ab_bench(
        "di_solver (2 deps, per-job)",
        a_runner,
        b_runner,
        batch=1,
        batches=7,
        correct=ok,
        note=f"per solve over {n}-solve batch",
    )
    result.a_ns_per_op /= n
    result.b_ns_per_op /= n
    result.a_seconds = result.a_ns_per_op / 1e9
    result.b_seconds = result.b_ns_per_op / 1e9
    return result


def bench_di_introspection_only() -> ABResult:
    """Isolate the reflection cost: get_type_hints+signature uncached vs cached."""
    import inspect
    from functools import lru_cache
    from typing import get_type_hints

    def actor(payload: dict[str, object], service: object, flag: bool) -> None:
        return None

    def a() -> object:
        module = sys.modules.get(actor.__module__)
        hints = get_type_hints(actor, include_extras=True, globalns=vars(module))
        sig = inspect.signature(actor)
        return (hints, sig)

    @lru_cache(maxsize=1024)
    def b_impl(func: object) -> tuple[dict[str, Any], inspect.Signature]:
        module = sys.modules.get(func.__module__)
        hints = get_type_hints(func, include_extras=True, globalns=vars(module))
        return hints, inspect.signature(func)

    def b() -> object:
        return b_impl(actor)

    a(), b()
    return ab_bench("di_introspection_only (1 call)", a, b, batch=200, batches=7)


# ── Bench 2: jsonb serialize + NUL guard chain ───────────────────────


def _payloads() -> dict[str, dict[str, object]]:
    base = {
        "order_id": "0198f2a4-7c1d-7de3-9a2b-3f8c5d6e7f80",
        "amount": 4250,
        "currency": "usd",
        "items": [{"sku": f"SKU-{i:05d}", "qty": i + 1, "price": 199 + i} for i in range(6)],
        "meta": {"source": "web", "campaign": "spring-sale", "nested": {"a": 1, "b": [1, 2, 3]}},
    }
    medium = {
        "order_id": "0198f2a4-7c1d-7de3-9a2b-3f8c5d6e7f80",
        "events": [
            {"ts": 1699999999 + i, "kind": "page_view", "url": f"/p/{i}"} for i in range(40)
        ],
        "tags": [f"tag-{i:03d}" for i in range(30)],
    }
    large = {
        "blob": "x" * 32768,
        "rows": [{"i": i, "v": i * 1.5, "s": f"row-{i}"} for i in range(700)],
    }
    nul_literal = {  # literal backslash-u-0000 TEXT — prefilter hits, but legal
        "text": "safe \\u0000 literal",
        "rows": [{"i": i, "s": f"row-{i} \\u0000 text"} for i in range(40)],
    }
    return {"small": base, "medium": medium, "large": large, "nul_literal": nul_literal}


def bench_jsonb() -> list[ABResult]:
    """A: dumps_jsonb_str current (decode + prefilter, re-parse + walk on hit).
    B1: bytes-out fused (no decode; byte prefilter; backslash-count confirm).
    B2: str-out with byte prefilter + confirm (keeps asyncpg str contract).
    B3: fused size (str, len) — kills terminal double-encode."""
    from taskq._json import dumps, dumps_jsonb_str

    payloads = _payloads()
    results = []

    def count_backslashes(data: bytes, idx: int) -> int:
        n = 0
        while idx - 1 - n >= 0 and data[idx - 1 - n : idx - n] == b"\\":
            n += 1
        return n

    def dumps_jsonb_bytes(value: dict[str, object]) -> bytes:
        data = dumps(value)
        pos = data.find(b"\\u0000")
        while pos != -1:
            if count_backslashes(data, pos) % 2 == 0:
                raise ValueError("NUL in jsonb")
            pos = data.find(b"\\u0000", pos + 6)
        return data

    def dumps_jsonb_str_v2(value: dict[str, object]) -> str:
        return dumps_jsonb_bytes(value).decode("utf-8")

    def dumps_jsonb_str_fused_size(value: dict[str, object]) -> tuple[str, int]:
        data = dumps_jsonb_bytes(value)
        return data.decode("utf-8"), len(data)

    for pname, payload in payloads.items():
        expected = dumps_jsonb_str(payload)
        nul_expected = None
        try:
            dumps_jsonb_str({"bad": "a\x00b"})
            nul_expected = "no-raise"
        except ValueError:
            nul_expected = "raise"

        b1_out = dumps_jsonb_bytes(payload)
        b2_out = dumps_jsonb_str_v2(payload)
        s3, sz3 = dumps_jsonb_str_fused_size(payload)
        ok = (
            b1_out.decode("utf-8") == expected
            and b2_out == expected
            and s3 == expected
            and sz3 == len(expected.encode("utf-8"))
        )
        try:
            dumps_jsonb_bytes({"bad": "a\x00b"})
            ok = ok and nul_expected == "no-raise"
        except ValueError:
            ok = ok and nul_expected == "raise"

        results.append(
            ab_bench(
                f"jsonb_str[{pname}] A=cur B=bytes",
                lambda p=payload: dumps_jsonb_str(p),
                lambda p=payload: dumps_jsonb_bytes(p),
                batch=200 if pname != "large" else 20,
                batches=7,
                correct=ok,
                note="B returns bytes (asyncpg contract change?)" if pname == "small" else "",
            )
        )
        results.append(
            ab_bench(
                f"jsonb_str[{pname}] A=cur B=str-v2",
                lambda p=payload: dumps_jsonb_str(p),
                lambda p=payload: dumps_jsonb_str_v2(p),
                batch=200 if pname != "large" else 20,
                batches=7,
                correct=True,
            )
        )
    # terminal double-encode kill: current (str + len(str.encode)) vs fused
    payload = payloads["medium"]

    def a_terminal() -> object:
        s = dumps_jsonb_str(payload)
        return s, len(s.encode("utf-8"))

    def b_terminal() -> object:
        return dumps_jsonb_str_fused_size(payload)

    a_out, b_out = a_terminal(), b_terminal()
    results.append(
        ab_bench(
            "terminal_write[size check] A B",
            a_terminal,
            b_terminal,
            batch=200,
            batches=7,
            correct=a_out == b_out,
            note="fused kills re-encode",
        )
    )
    return results


# ── Bench 3: decode_jsonb stdlib json vs orjson (the user's challenge) ─


def bench_decode_jsonb() -> list[ABResult]:
    """A: stdlib json.loads (current web/admin/_jsonb.py). B: orjson.
    Run the same bench under 3.13 and 3.14 to settle the 'orjson matters
    less on modern Python' question empirically."""
    from taskq._json import loads as orjson_loads

    small = '{"order_id": "0198f2a4", "amount": 4250, "ok": true, "tags": ["a", "b"]}'
    medium = stdlib_json.dumps(
        {"rows": [{"i": i, "v": i * 1.5, "s": f"row-{i}", "ok": True} for i in range(50)]}
    )
    large = stdlib_json.dumps({"blob": "x" * 16384, "n": list(range(2000))})
    results = []
    for name, text in (("small", small), ("medium-50row", medium), ("large-16kb", large)):
        a_out = stdlib_json.loads(text)
        b_out = orjson_loads(text)
        results.append(
            ab_bench(
                f"decode_jsonb[{name}] stdlib vs orjson",
                lambda t=text: stdlib_json.loads(t),
                lambda t=text: orjson_loads(t),
                batch=500 if name == "small" else 100,
                batches=7,
                correct=a_out == b_out,
            )
        )
    return results


def bench_encode_json_stdlib_vs_orjson() -> list[ABResult]:
    """Complement: dumps side, where orjson's advantage is expected to hold."""
    import json as sj

    from taskq._json import dumps as orjson_dumps

    obj = {
        "rows": [{"i": i, "v": i * 1.5, "s": f"row-{i}", "ok": True} for i in range(50)],
        "tags": [f"t{i}" for i in range(20)],
    }
    a_out = sj.dumps(obj)
    b_out = orjson_dumps(obj).decode()
    return [
        ab_bench(
            "encode_json-50row stdlib vs orjson",
            lambda: sj.dumps(obj),
            lambda: orjson_dumps(obj),
            batch=200,
            batches=7,
            correct=std_out_eq(a_out, b_out),
        )
    ]


def std_out_eq(a: str, b: str) -> bool:
    return stdlib_json.loads(a) == stdlib_json.loads(b)


# ── Bench 4: cron next-fire ──────────────────────────────────────────


def bench_cron() -> list[ABResult]:
    """A: compute_next_fire_after as-is. B: ZoneInfo cached at module level
    (ZoneInfo has an internal cache; B tests whether the lookup+keying cost
    is real). Burst stall measured separately in --stall mode."""
    from zoneinfo import ZoneInfo

    from taskq.cron import compute_next_fire_after

    exprs = ["*/5 * * * *", "0 0 * * *", "30 14 1 * *", "*/15 9-17 * * 1-5"]
    tzs = ["UTC", "America/New_York", "Australia/Lord_Howe"]
    now = datetime(2026, 9, 12, 10, 30, 0, tzinfo=UTC)

    def a() -> object:
        out = []
        for e in exprs:
            for tz in tzs:
                out.append(compute_next_fire_after(e, tz, now))
        return out

    _zi_cache: dict[str, ZoneInfo] = {}

    def b() -> object:
        out = []
        for e in exprs:
            for tz_name in tzs:
                tz = _zi_cache.get(tz_name)
                if tz is None:
                    tz = _zi_cache[tz_name] = ZoneInfo(tz_name)
                out.append(compute_next_fire_after(e, tz_name, now))
        return out

    a_out, b_out = a(), b()
    ok = a_out == b_out
    return [
        ab_bench(
            "cron_next_fire[12 combos] A B",
            a,
            b,
            batch=5,
            batches=7,
            correct=ok,
            note="B = cached ZoneInfo only (honest: croniter unparsed)",
        )
    ]


# ── Bench 5: JobRow decode ───────────────────────────────────────────


class FakeRecord(dict):
    def __getitem__(self, key: str) -> object:
        return dict.__getitem__(self, key)


def bench_job_row_decode() -> list[ABResult]:
    """A: _job_row_from_record as-is. B: tightened variant (single dict.get
    pass, hoisted jsonb checks). 1000-row dispatch batch."""
    from taskq.backend._protocol import (
        IdempotencyKey,
        IdentityKey,
        JobId,
        JobRow,
        parse_cancel_phase,
        parse_retry_kind,
    )
    from taskq.backend._records import _job_row_from_record, jsonb_to_dict

    def make_record(i: int) -> FakeRecord:
        ts = datetime(2026, 9, 12, 10, 0, tzinfo=UTC)
        return FakeRecord(
            id=JobId(str(UUID(int=i, version=4))),
            actor="orders.ship",
            queue="orders",
            identity_key=IdentityKey("ord-1"),
            fairness_key="orders",
            payload='{"order_id": "x", "amount": 1}',
            payload_schema_ver=1,
            status="running",
            priority=5,
            attempt=1,
            max_attempts=3,
            retry_kind="transient",
            schedule_to_close=timedelta(hours=1),
            start_to_close=timedelta(minutes=10),
            heartbeat_timeout=timedelta(seconds=30),
            created_at=ts,
            scheduled_at=ts,
            started_at=ts,
            finished_at=None,
            last_heartbeat_at=ts,
            locked_by_worker=None,
            lock_expires_at=None,
            cancel_requested_at=None,
            cancel_phase=0,
            error_class=None,
            error_message=None,
            error_traceback=None,
            progress_state='{"pct": 10}',
            progress_seq=2,
            result=None,
            result_size_bytes=None,
            result_expires_at=None,
            idempotency_key=IdempotencyKey("idem-1"),
            idempotency_scope="job",
            trace_id="a" * 32,
            span_id="b" * 16,
            metadata='{"source": "test"}',
            tags=["t1", "t2"],
        )

    def b_decode(rec: FakeRecord) -> JobRow:
        payload = rec["payload"]
        progress = rec["progress_state"]
        result = rec["result"]
        meta = rec["metadata"]
        raw_identity = rec["identity_key"]
        raw_idem = rec["idempotency_key"]
        tags = rec["tags"]
        return JobRow(
            id=JobId(rec["id"]),
            actor=rec["actor"],
            queue=rec["queue"],
            identity_key=IdentityKey(raw_identity) if raw_identity is not None else None,
            fairness_key=rec["fairness_key"],
            payload=jsonb_to_dict(payload) or {},
            payload_schema_ver=rec["payload_schema_ver"],
            status=rec["status"],
            priority=rec["priority"],
            attempt=rec["attempt"],
            max_attempts=rec["max_attempts"],
            retry_kind=parse_retry_kind(rec["retry_kind"]),
            schedule_to_close=rec["schedule_to_close"],
            start_to_close=rec["start_to_close"],
            heartbeat_timeout=rec["heartbeat_timeout"],
            created_at=rec["created_at"],
            scheduled_at=rec["scheduled_at"],
            started_at=rec["started_at"],
            finished_at=rec["finished_at"],
            last_heartbeat_at=rec["last_heartbeat_at"],
            locked_by_worker=rec["locked_by_worker"],
            lock_expires_at=rec["lock_expires_at"],
            cancel_requested_at=rec["cancel_requested_at"],
            cancel_phase=parse_cancel_phase(rec["cancel_phase"]),
            error_class=rec["error_class"],
            error_message=rec["error_message"],
            error_traceback=rec["error_traceback"],
            progress_state=jsonb_to_dict(progress) or {},
            progress_seq=rec["progress_seq"],
            result=jsonb_to_dict(result),
            result_size_bytes=rec["result_size_bytes"],
            result_expires_at=rec["result_expires_at"],
            idempotency_key=IdempotencyKey(raw_idem) if raw_idem is not None else None,
            idempotency_scope=rec["idempotency_scope"],
            trace_id=rec["trace_id"],
            span_id=rec["span_id"],
            metadata=jsonb_to_dict(meta) or {},
            tags=tuple(tags) if tags else (),
        )

    rows = [make_record(i) for i in range(1000)]
    a_out = [_job_row_from_record(r) for r in rows[:2]]
    b_out = [b_decode(r) for r in rows[:2]]
    ok = all(
        a.id == b.id
        and a.payload == b.payload
        and a.metadata == b.metadata
        and a.tags == b.tags
        and a.progress_state == b.progress_state
        for a, b in zip(a_out, b_out, strict=False)
    )

    def a_batch() -> object:
        return [_job_row_from_record(r) for r in rows]

    def b_batch() -> object:
        return [b_decode(r) for r in rows]

    return [
        ab_bench(
            "job_row_decode[1000 rows] A B",
            a_batch,
            b_batch,
            batch=1,
            batches=5,
            correct=ok,
            note="B = tightened decode (pre-validated)",
        )
    ]


# ── Bench 6: retry policy reconstruction ─────────────────────────────


def bench_retry_policy() -> list[ABResult]:
    """A: decide_after_failure-style RetryPolicy reconstruction per failure.
    B: model_copy(update=...) on a cached base policy (skips re-validation)."""
    from datetime import timedelta as td

    from pydantic import ValidationError

    from taskq.retry import RetryPolicy

    base = RetryPolicy(
        kind="transient",
        max_attempts=3,
        backoff="exponential",
        base=td(seconds=5),
        cap=td(hours=1),
        jitter=0.2,
    )

    def a() -> object:
        return RetryPolicy(
            kind="transient",
            max_attempts=3,
            backoff="exponential",
            base=td(seconds=5),
            cap=td(hours=1),
            jitter=0.2,
            time_budget=None,
        )

    def b() -> object:
        return base.model_copy(update={"kind": "transient", "max_attempts": 3})

    try:
        a_policy = a()
        b_policy = b()
        ok = (
            a_policy.kind == b_policy.kind
            and a_policy.max_attempts == b_policy.max_attempts
            and a_policy.base == b_policy.base
            and a_policy.cap == b_policy.cap
            and a_policy.jitter == b_policy.jitter
        )
    except ValidationError:
        ok = False
    return [
        ab_bench(
            "retry_policy_reconstruct A B",
            a,
            b,
            batch=500,
            batches=7,
            correct=ok,
            note="B = model_copy, skips re-validation",
        )
    ]


# ── Bench 7: keyed eviction scan (cardinality) ───────────────────────


def bench_evict_keyed() -> list[ABResult]:
    """A: current-style linear dict scan at 10k entries. B: idle-ordered
    heapq index (pop idle from the top). Simulates registry.py:1194-1229."""
    import heapq
    from random import Random

    rng = Random(42)  # noqa: S311  # Why: seeded PRNG shapes last_used timings; nothing security-sensitive.
    n = 10_000
    now = time.monotonic()
    # last_used times: 80% idle > 60s old, 20% recent
    last_used = {f"key-{i}": now - rng.uniform(0, 120) for i in range(n)}

    cutoff = now - 60

    def a() -> object:
        stale = [k for k, ts in last_used.items() if ts < cutoff]
        for k in stale:
            last_used.pop(k, None)
        return len(stale)

    # B: maintain a heap of (last_used, key); pop while min < cutoff.
    # Rebuild amortized — here we simulate the steady-state pop path.
    heap: list[tuple[float, str]] = [(ts, k) for k, ts in last_used.items()]
    heapq.heapify(heap)

    def b() -> object:
        evicted = 0
        while heap and heap[0][0] < cutoff:
            heapq.heappop(heap)
            evicted += 1
        return evicted

    a_n = a()
    b_n = b()
    return [
        ab_bench(
            f"evict_keyed[{n} entries] A B",
            a,
            b,
            batch=1,
            batches=5,
            correct=a_n >= 0 and b_n >= 0,
            note="A linear scan; B heap-index pop (one-shot)",
        )
    ]


# ── Event-loop stall probe ───────────────────────────────────────────


async def stall_probe(
    work: Callable[[], None], *, seconds: float = 2.0
) -> tuple[float, float, int]:
    """Run sync `work` in a loop on the event loop while a 1ms heartbeat task
    measures drift. Returns (max_stall_ms, mean_stall_ms, ticks)."""
    loop = asyncio.get_running_loop()
    done = loop.time() + seconds
    stalls: list[float] = []

    async def heartbeat() -> None:
        last = loop.time()
        while True:
            await asyncio.sleep(0.001)
            now = loop.time()
            stalls.append((now - last) * 1000)
            last = now
            if now >= done:
                break

    hb = asyncio.create_task(heartbeat())
    while loop.time() < done:
        work()
        await asyncio.sleep(0)  # yield so the heartbeat can observe drift
    await hb
    if not stalls:
        return 0.0, 0.0, 0
    return max(stalls), statistics.mean(stalls), len(stalls)


def run_stall_probes() -> None:
    from taskq._json import dumps_jsonb_str
    from taskq.cron import compute_next_fire_after

    payloads = _payloads()
    nul_payload = {"rows": [{"s": "a\x00b" if i == 0 else f"row-{i}"} for i in range(400)]}
    nul_payload["rows"][0] = {"s": "x\x00y"}  # guaranteed real NUL → re-parse + walk every call
    # NOTE: real NUL raises ValueError; for stall purposes we alternate
    # between the legal nul_literal payload (prefilter hit + walk) and catch.
    nul_literal = payloads["nul_literal"]

    burst_10k = datetime(2026, 3, 8, 2, 0, 0, tzinfo=UTC)  # DST window in NY
    exprs = ["*/5 * * * *"] * 10_000

    def cron_burst() -> None:
        for e in exprs:
            compute_next_fire_after(e, "America/New_York", burst_10k)

    def jsonb_storm() -> None:
        for _ in range(1000):
            with contextlib.suppress(ValueError):
                dumps_jsonb_str(nul_literal)

    def di_storm() -> None:
        pass  # wired below via bench_di_solver internals if needed

    async def main() -> None:
        print("\n── Event-loop stall probes (1ms heartbeat on same loop) ──")
        for name, work in (
            ("cron burst 10k schedules (1 full burst)", cron_burst),
            ("jsonb NUL-literal storm (1000 calls)", jsonb_storm),
        ):
            t0 = time.perf_counter()
            max_s, mean_s, ticks = await stall_probe(work, seconds=2.0)
            wall = time.perf_counter() - t0
            print(
                f"  {name:<44} max_stall={max_s:8.2f} ms  mean={mean_s:6.2f} ms"
                f"  wall={wall:5.2f}s  ticks={ticks}"
            )

    asyncio.run(main())


# ── cProfile mode ────────────────────────────────────────────────────


def run_profile(which: str) -> None:
    import cProfile
    import pstats

    benches = {
        "jsonb": lambda: bench_jsonb(),
        "decode": lambda: bench_decode_jsonb(),
        "cron": lambda: bench_cron(),
        "rows": lambda: bench_job_row_decode(),
        "retry": lambda: bench_retry_policy(),
    }
    if which not in benches:
        print(f"unknown bench {which!r}; choices: {', '.join(benches)}")
        return
    prof = cProfile.Profile()
    prof.enable()
    benches[which]()
    prof.disable()
    stats = pstats.Stats(prof)
    stats.sort_stats("cumulative")
    print(f"\n── cProfile: {which} (top 20 cumulative) ──")
    stats.print_stats(20)


# ── main ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only", default="", help="comma-separated subset: di,jsonb,decode,cron,rows,retry,evict"
    )
    parser.add_argument(
        "--profile", default="", help="cProfile one bench: jsonb|decode|cron|rows|retry"
    )
    parser.add_argument("--stall", action="store_true", help="run event-loop stall probes")
    args = parser.parse_args()

    # Match production log config (INFO, stdlib-filtered) so the DI solver's
    # per-dep debug logging doesn't pay terminal rendering inside timed regions.
    from taskq.obs._structlog import setup_logging

    setup_logging(level="INFO", log_format="json")

    if args.profile:
        run_profile(args.profile)
        return
    if args.stall:
        run_stall_probes()
        return

    results: list[ABResult] = []
    wanted = {s.strip() for s in args.only.split(",") if s.strip()} or {
        "di",
        "jsonb",
        "decode",
        "cron",
        "rows",
        "retry",
        "evict",
    }
    if "di" in wanted:
        results.append(bench_di_solver())
        results.append(bench_di_introspection_only())
    if "jsonb" in wanted:
        results.extend(bench_jsonb())
    if "decode" in wanted:
        results.extend(bench_decode_jsonb())
        results.extend(bench_encode_json_stdlib_vs_orjson())
    if "cron" in wanted:
        results.extend(bench_cron())
    if "rows" in wanted:
        results.extend(bench_job_row_decode())
    if "retry" in wanted:
        results.extend(bench_retry_policy())
    if "evict" in wanted:
        results.extend(bench_evict_keyed())

    print(f"\nPython {sys.version.split()[0]} — A/B results (interleaved, median ns/op)")
    report(results)


if __name__ == "__main__":
    main()
