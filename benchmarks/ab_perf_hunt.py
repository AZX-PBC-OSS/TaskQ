"""A/B benches for the seams that landed since the last perf proof.

Follows the bench_hotspots.py harness pattern (interleaved batches, medians,
correctness assertions) for the new seams:

- the cancel ladder's full-poll walk (held walk over every active job per
  heartbeat tick) and its unheld sighting map,
- the identity-fenced registry maps (register/deregister under the
  asyncio.Lock, held_ids snapshots),
- the shield-retrieved closes (per-job-exit shield_with_retrieval cost),
- the poll-state endpoint's per-tick parse+re-dump (the server-side twin of
  the client's per-tick JSON canonicalize) and its seq ETag.

Async benches are timed per batch inside one run_until_complete call, so the
loop's own startup cost is amortized over `batch` operations.

Usage:
    python benchmarks/ab_perf_hunt.py                 # all benches
    python benchmarks/ab_perf_hunt.py --only ladder   # subset by name
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from types import SimpleNamespace

from taskq._ids import new_uuid
from taskq._shield import (  # pyright: ignore[reportPrivateUsage]  # Why: the bench times the exact callback the cancel path attaches.
    _log_detached_failure,
    shield_with_retrieval,
)
from taskq.backend._protocol import CancelPhase, JobId
from taskq.worker.cancel import ActiveJobRegistry, _ActiveJob, _CancelController

# ── harness (same conventions as bench_hotspots.ab_bench, async-aware) ──


class StubConn:
    """A conn whose fetch returns a canned row list with no I/O."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return self._rows

    async def execute(self, sql: str, *args: object) -> str:
        return "UPDATE 1"


def _make_deps(n_jobs: int, registry: ActiveJobRegistry) -> SimpleNamespace:
    settings = SimpleNamespace(
        schema_name="tq_bench",
        cancellation_grace_period=1.0,
        cleanup_grace_period=1.0,
        heartbeat_interval=0.5,
    )
    liveness = SimpleNamespace(tick=lambda *a, **k: None)
    for _ in range(n_jobs):
        job_id: JobId = new_uuid()
        task = asyncio.get_event_loop().create_task(_noop())
        entry = _ActiveJob(
            job_id=job_id,
            task=task,
            ctx=SimpleNamespace(  # type: ignore[arg-type]
                cancel_event=asyncio.Event(),
                _abort_requested=asyncio.Event(),
            ),
        )
        registry._by_id[job_id] = entry  # pyright: ignore[reportPrivateUsage]  # Why: the bench plants idle entries directly, the register coroutine's await is not what is measured.
    return SimpleNamespace(settings=settings, active_jobs=registry, liveness=liveness)


async def _noop() -> None:
    await asyncio.Event().wait()


def _make_controller(n_jobs: int) -> tuple[_CancelController, StubConn]:
    registry = ActiveJobRegistry()
    deps = _make_deps(n_jobs, registry)
    conn = StubConn([])
    controller = _CancelController(
        deps,  # type: ignore[arg-type]
        new_uuid(),
        SimpleNamespace(),  # type: ignore[arg-type]  # Why: run_in_tx never touches the backend on the empty-poll path.
    )
    return controller, conn


def time_async_batches(
    coro_fn: object,
    batch: int,
    batches: int,
    loop: asyncio.AbstractEventLoop,
) -> list[float]:
    times: list[float] = []

    async def one_batch() -> None:
        for _ in range(batch):
            await coro_fn()  # type: ignore[operator]

    for _ in range(batches):
        t0 = time.perf_counter_ns()
        loop.run_until_complete(one_batch())
        times.append((time.perf_counter_ns() - t0) / batch)
    return times


def ab_async(
    name: str,
    a_fn: object,
    b_fn: object,
    *,
    batch: int,
    batches: int = 9,
    correct: bool = True,
    note: str = "",
) -> tuple[str, float, float, bool, str]:
    loop = asyncio.new_event_loop()
    try:
        # warmup
        time_async_batches(a_fn, 20, 2, loop)
        time_async_batches(b_fn, 20, 2, loop)
        a_times: list[float] = []
        b_times: list[float] = []
        for _ in range(batches):
            a_times.extend(time_async_batches(a_fn, batch, 1, loop))
            b_times.extend(time_async_batches(b_fn, batch, 1, loop))
    finally:
        loop.close()
    a_med = statistics.median(a_times)
    b_med = statistics.median(b_times)
    return (
        name,
        a_med,
        b_med,
        correct,
        note + f" [A spread {(max(a_times) - min(a_times)) / a_med:.0%}]",
    )


def ab_sync(
    name: str,
    a_fn: object,
    b_fn: object,
    *,
    batch: int,
    batches: int = 9,
    correct: bool = True,
    note: str = "",
) -> tuple[str, float, float, bool, str]:
    def time_sync(fn: object) -> list[float]:
        times: list[float] = []
        for _ in range(batches):
            t0 = time.perf_counter_ns()
            for _ in range(batch):
                fn()
            times.append((time.perf_counter_ns() - t0) / batch)
        return times

    time_sync(a_fn)
    time_sync(b_fn)
    a_times = time_sync(a_fn)
    b_times = time_sync(b_fn)
    a_med = statistics.median(a_times)
    b_med = statistics.median(b_times)
    return (
        name,
        a_med,
        b_med,
        correct,
        note + f" [A spread {(max(a_times) - min(a_times)) / a_med:.0%}]",
    )


# ── 1. the cancel ladder's full-poll walk ──────────────────────────────


def bench_ladder_idle(n_jobs: int) -> tuple[str, float, float, bool, str]:
    """The heartbeat tick's cancel hook with a poll that returns NO rows.

    A: the real run_in_tx - fetch, then the held walk over ALL n_jobs
       active entries (dict lookups and phase arithmetic per entry).
    B: variant - the same fetch guarded by `not rows and nothing
       mid-ladder`: when the poll is empty and no entry carries a
       non-NONE phase, no arm of the ladder can fire, so the walk is
       skipped wholesale.
    """
    controller, conn = _make_controller(n_jobs)

    async def a() -> None:
        await controller.run_in_tx(conn)  # type: ignore[arg-type]

    c2, conn2 = _make_controller(n_jobs)

    async def b() -> None:
        rows = await conn2.fetch("", c2._worker_id)  # pyright: ignore[reportPrivateUsage]  # Why: the variant is the guard + identical walk; the stub conn ignores the SQL.
        if not rows:
            for active in c2._deps.active_jobs.all():  # pyright: ignore[reportPrivateUsage]
                if active.cancel_phase != CancelPhase.NONE or active.cancel_observed_at is not None:
                    break
            else:
                return
        await c2.run_in_tx(conn2)  # type: ignore[arg-type]

    return ab_async(
        f"cancel_ladder_idle[{n_jobs} jobs, 0 rows]",
        a,
        b,
        batch=200,
        note="B: skip the held walk when the poll is empty and no entry is mid-ladder",
    )


def bench_unheld_walk(n_rows: int) -> tuple[str, float, float, bool, str]:
    """The unheld sighting walk's shape, all polled rows held (the common case).

    A: the current shape - a list comp calling active_jobs.get per row,
       then (when either side is non-empty) set(unheld_ids) and the
       set(stamps) difference.
    B: variant - build the `polled` set lazily and skip the difference
       when the sighting map is empty.
    """
    registry = ActiveJobRegistry()
    _make_deps(n_rows, registry)
    rows: list[dict[str, object]] = [{"id": jid, "cancel_phase": 0} for jid in registry._by_id]  # pyright: ignore[reportPrivateUsage]
    stamps: dict[JobId, float] = {}

    def a() -> int:
        unheld_ids = [
            row["id"]
            for row in rows
            if registry.get(row["id"]) is None  # type: ignore[arg-type]
        ]
        counted = 0
        if unheld_ids or stamps:
            polled = set(unheld_ids)
            for stale_id in set(stamps) - polled:
                del stamps[stale_id]
            for unheld_id in unheld_ids:
                counted += 1
                stamps.setdefault(unheld_id, 0.0)  # type: ignore[arg-type]
        return counted

    def b() -> int:
        unheld_ids = [
            row["id"]
            for row in rows
            if registry.get(row["id"]) is None  # type: ignore[arg-type]
        ]
        counted = 0
        if stamps:
            polled = set(unheld_ids)
            for stale_id in set(stamps) - polled:
                del stamps[stale_id]
        for unheld_id in unheld_ids:
            counted += 1
            stamps.setdefault(unheld_id, 0.0)  # type: ignore[arg-type]
        return counted

    name, a_ns, b_ns, ok, note = ab_sync(
        f"unheld_sighting_walk[{n_rows} rows, all held]",
        a,
        b,
        batch=500,
        note="B: lazy polled-set, skip the stamp diff when the map is empty",
    )
    assert a() == b(), "output mismatch"
    return name, a_ns, b_ns, ok, note


# ── 2. the identity-fenced registry maps ───────────────────────────────


def bench_registry_exit(n_pairs: int) -> tuple[str, float, float, bool, str]:
    """Per-job-exit cost: the identity-fenced deregister vs the bare pop.

    A: real `await deregister(job_id, entry)` (lock + identity check).
    B: the pre-fence bare shape `dict.pop(job_id, None)`.
    """
    registry = ActiveJobRegistry()
    entries: list[tuple[JobId, _ActiveJob]] = []

    async def seed_and_drain_a() -> None:
        for _ in range(n_pairs):
            job_id: JobId = new_uuid()
            task = asyncio.get_running_loop().create_task(_noop())
            entry = _ActiveJob(job_id=job_id, task=task, ctx=SimpleNamespace())  # type: ignore[arg-type]
            await registry.register(job_id, task, entry.ctx)  # type: ignore[arg-type]
            entries.append((job_id, entry))
        for job_id, entry in entries:
            await registry.deregister(job_id, entry)
        entries.clear()

    plain: dict[JobId, _ActiveJob] = {}

    async def seed_and_drain_b() -> None:
        for _ in range(n_pairs):
            job_id = new_uuid()
            task = asyncio.get_running_loop().create_task(_noop())
            entry = _ActiveJob(job_id=job_id, task=task, ctx=SimpleNamespace())  # type: ignore[arg-type]
            plain[job_id] = entry
            entries.append((job_id, entry))
        for job_id, _entry in entries:
            plain.pop(job_id, None)
        entries.clear()

    return ab_async(
        f"registry_register+deregister[{n_pairs} pairs]",
        seed_and_drain_a,
        seed_and_drain_b,
        batch=10,
        note="B: bare-keyed set/pop (the pre-fence shape)",
    )


def bench_held_ids(n_jobs: int) -> tuple[str, float, float, bool, str]:
    registry = ActiveJobRegistry()
    _make_deps(n_jobs, registry)

    def a() -> list[JobId]:
        return registry.held_ids()

    by_id = registry._by_id  # pyright: ignore[reportPrivateUsage]
    intents: dict[JobId, object] = {}

    def b() -> list[JobId]:
        return [*by_id, *intents]

    name, a_ns, b_ns, ok, note = ab_sync(
        f"registry_held_ids[{n_jobs} entries]",
        a,
        b,
        batch=2000,
        note="B: inline list splices (method-call overhead removed)",
    )
    assert a() == b()
    return name, a_ns, b_ns, ok, note


# ── 3. the shield-retrieved closes ─────────────────────────────────────


async def _tiny_write() -> bool:
    return True


def bench_shield_close() -> tuple[str, float, float, bool, str]:
    """A: `await shield_with_retrieval(write())` - every job exit's shape.
    B: `await write()` - the bare await.
    C: variant - ensure_future + bare `await task` + the retrieval
       callback on the cancel path. The inner is an anonymous task nobody
       else holds, so outer cancellation detaches it exactly as shield
       does, minus shield's wrapper future."""

    async def a() -> object:
        return await shield_with_retrieval(_tiny_write())

    async def b() -> object:
        return await _tiny_write()

    async def c() -> object:
        task = asyncio.ensure_future(_tiny_write())
        try:
            return await task
        except asyncio.CancelledError:
            task.add_done_callback(_log_detached_failure)
            raise

    return ab_async(
        "shield_retrieved_close[per job exit]",
        a,
        b,
        batch=500,
        note="B: bare await (the pre-shield shape); see shield_retrieved_close_c for the variant",
    )


def bench_shield_close_c() -> tuple[str, float, float, bool, str]:
    async def a() -> object:
        return await shield_with_retrieval(_tiny_write())

    async def c() -> object:
        task = asyncio.ensure_future(_tiny_write())
        try:
            return await task
        except asyncio.CancelledError:
            task.add_done_callback(_log_detached_failure)
            raise

    return ab_async(
        "shield_retrieved_close_c[DISQUALIFIED variant]",
        a,
        c,
        batch=500,
        note="C is disqualified: await task propagates the outer cancel INTO the inner task "
        "(the outer's _fut_waiter IS the task), stranding nothing but cancelling the "
        "in-flight write, the exact defect the shield exists to prevent. Timing kept "
        "only as the record of why the 1.75x is not taken.",
    )


def bench_lock_only() -> tuple[str, float, float, bool, str]:
    """The registry's uncontended asyncio.Lock acquire/release vs no lock."""
    lock = asyncio.Lock()
    plain: dict[JobId, object] = {}
    entry = object()
    job_id: JobId = new_uuid()

    async def a() -> None:
        async with lock:
            if plain.get(job_id) is not entry:
                plain[job_id] = entry

    async def b() -> None:
        plain[job_id] = entry

    return ab_async(
        "registry_lock_acquire_release",
        a,
        b,
        batch=2000,
        note="B: the bare-keyed write (no lock, no fence)",
    )


# ── 4. the poll-state endpoint's per-tick work (the ETag twin) ─────────


def _progress_payload(fields: int, data_items: int) -> dict[str, object]:
    state: dict[str, object] = {"step": f"step-{fields}", "percent": 42, "detail": "x" * 64}
    if data_items:
        state["data"] = [
            {"sku": f"SKU-{i:03d}", "qty": i % 5 + 1, "price": i * 1.5} for i in range(data_items)
        ]
    state["ts"] = "2026-09-23T12:00:00Z"
    return state


def bench_poll_state_twin(data_items: int) -> tuple[str, float, float, bool, str]:
    """The poll-state endpoint's serialization work per tick.

    A: the current shape - asyncpg's jsonb str is parsed (loads) and the
       response dict is re-encoded (dumps).
    B: variant - the raw jsonb text is embedded verbatim via
       orjson.Fragment; zero parse, zero re-encode of the state.
    """
    import orjson

    from taskq._json import dumps_str, loads

    state = _progress_payload(3, data_items)
    raw_jsonb = dumps_str(state)

    def a() -> bytes:
        parsed = loads(raw_jsonb)
        progress_state = parsed if isinstance(parsed, dict) else None
        return orjson.dumps(
            {
                "status": "running",
                "progress_state": progress_state,
                "progress_seq": 7,
            }
        )

    def b() -> bytes:
        return orjson.dumps(
            {
                "status": "running",
                "progress_state": orjson.Fragment(raw_jsonb.encode()),
                "progress_seq": 7,
            }
        )

    ok = a() == b()
    return ab_sync(
        f"poll_state_twin[data x{data_items}]",
        a,
        b,
        batch=2000,
        correct=ok,
        note="B: Fragment passthrough of asyncpg's raw jsonb text",
    )


def bench_etag() -> tuple[str, float, float, bool, str]:
    """The ETag computation itself: f-string + header compare."""
    headers = {"if-none-match": '"7"'}

    def a() -> bool:
        etag = f'"{7}"'
        return headers.get("if-none-match") == etag

    def b() -> bool:
        return headers.get("if-none-match") == '"7"'

    return ab_sync(
        "etag_seq_compute",
        a,
        b,
        batch=5000,
        note="informational: the ETag itself is an f-string",
    )


# ── 5. the client's per-tick JSON canonicalize (node bench, reported) ──


def report(results: list[tuple[str, float, float, bool, str]]) -> None:
    print(f"\n{'bench':<46} {'A (current)':>14} {'B (variant)':>14} {'speedup':>9}  ok")
    print("-" * 100)
    for name, a_ns, b_ns, ok, note in results:
        a = f"{a_ns:,.0f} ns" if a_ns < 1e6 else f"{a_ns / 1e9 * 1e3:,.2f} ms"
        b = f"{b_ns:,.0f} ns" if b_ns < 1e6 else f"{b_ns / 1e9 * 1e3:,.2f} ms"
        flag = "OK" if ok else "MISMATCH!"
        print(f"{name:<46} {a:>14} {b:>14} {a_ns / b_ns:>8.2f}x  {flag}  {note}")


BENCHES = {
    "ladder50": lambda: bench_ladder_idle(50),
    "ladder500": lambda: bench_ladder_idle(500),
    "unheld50": lambda: bench_unheld_walk(50),
    "registry2": lambda: bench_registry_exit(2),
    "registry20": lambda: bench_registry_exit(20),
    "held50": lambda: bench_held_ids(50),
    "shield": bench_shield_close,
    "shield_c": bench_shield_close_c,
    "lock": bench_lock_only,
    "twin_small": lambda: bench_poll_state_twin(0),
    "twin_big": lambda: bench_poll_state_twin(24),
    "etag": bench_etag,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default="", help="comma-separated bench names")
    args = parser.parse_args()
    only = {s.strip() for s in args.only.split(",") if s.strip()}
    results = [fn() for key, fn in BENCHES.items() if not only or key in only]
    report(results)


if __name__ == "__main__":
    main()
