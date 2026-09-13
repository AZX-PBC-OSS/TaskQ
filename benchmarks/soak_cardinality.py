"""Keyed-ref cardinality soak for the rate-limit registry (no Postgres, no Redis).

Drives the REAL ``RateLimitRegistry`` keyed-ref machinery
(``_resolve_rate_limit_name`` / ``_resolve_reservation_name`` /
``evict_idle_keyed_*`` / the opportunistic-eviction gate) entirely
in memory — the materialization path with ``pg_pool=None`` never touches a
backend, so the tracking-dict + eviction mechanics can be soaked directly.

Wall clock is COMPRESSED: the registry module's ``monotonic`` is patched
(benchmark-side only, ``taskq.ratelimit.registry.monotonic``) with a virtual
clock, so 2.5 virtual hours run in ~tens of real seconds while every stamp,
sweep cutoff and gate check executes the production code with production
semantics.

Timeline (virtual seconds):
  Phase 1  t=0..3600    100k distinct keyed refs arrive (80k rate-limit +
                        20k reservation keys), ~22.2 + 5.6 per virtual second;
                        a recurring pool of 500 keys per kind is re-acquired
                        every 30s (keeps those entries live forever).
  Phase 2  t=3600..9000 quiet: only the recurring pools are touched; the
                        per-worker 30s sweeps must reclaim everything else.

Worker-faithful mechanics replicated here:
  - sweep cadence 30s, calling BOTH ``evict_idle_keyed_reservations`` and
    ``evict_idle_keyed_rate_limits`` with the production 1-hour idle threshold;
  - opportunistic eviction on the acquire path when a cap would be exceeded
    (registry-gated to once per 30s — the gate under test).

Measured: peak tracking-dict sizes vs the 10k caps, eviction scan cost over
time, denial-path latency (the O(1)-amortized claim), post-eviction steady
state, and memory trend (tracemalloc; the ``--memory`` run).

Run:
    .venv/bin/python benchmarks/soak_cardinality.py            # latency/cost run
    .venv/bin/python benchmarks/soak_cardinality.py --memory   # memory-trend run
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import resource
import statistics
import sys
import time
import tracemalloc
from datetime import timedelta
from pathlib import Path

from pydantic import BaseModel

from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings


def _registry_module() -> object:
    """Return the real ``taskq.ratelimit.registry`` MODULE object.

    ``import taskq.ratelimit.registry as m`` does NOT give the module: the
    package's ``__init__`` re-exports the module-level ``registry``
    singleton under the same name, and attribute access shadows the
    submodule binding. Patching attributes on the singleton (as an earlier
    draft did) silently misses every module-global lookup.
    """
    return sys.modules["taskq.ratelimit.registry"]


# ---------------------------------------------------------------------------
# Scenario constants (virtual seconds unless noted)
# ---------------------------------------------------------------------------

STREAMING_PHASE_SECONDS = 3600.0  # 1 h of compressed worker activity
TOTAL_VIRTUAL_SECONDS = 9000.0  # + 1.5 h quiet tail for reclaim observation
SWEEP_CADENCE_SECONDS = 30.0
OPPORTUNISTIC_GATE_SECONDS = 30.0  # registry's _OPPORTUNISTIC_EVICT_MIN_INTERVAL
IDLE_THRESHOLD = timedelta(hours=1)  # production _KEYED_IDLE_THRESHOLD
POOL_SIZE = 500  # recurring keys per kind, touched every sweep cadence
STREAMING_RL_KEYS = 80_000
STREAMING_RES_KEYS = 20_000
CAP = 10_000  # production default settings.max_keyed_{rate_limits,reservations}


class SoakPayload(BaseModel):
    tenant_id: str


def _make_refs() -> tuple[KeyedRateLimitRef, KeyedReservationRef]:
    rl_ref = KeyedRateLimitRef.typed(
        SoakPayload,
        base_name="soak.rl",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )
    res_ref = KeyedReservationRef.typed(
        SoakPayload,
        base_name="soak.res",
        key_fn=lambda p: p.tenant_id,
        slots=1,
        lease=timedelta(minutes=5),
    )
    return rl_ref, res_ref


class VirtualClock:
    """Benchmark-side replacement for ``taskq.ratelimit.registry.monotonic``."""

    def __init__(self) -> None:
        self.now_virtual = 0.0

    def __call__(self) -> float:
        return self.now_virtual


@dataclasses.dataclass
class SweepSample:
    t: float
    tracked_res: int
    tracked_rl: int
    evicted_res: int
    evicted_rl: int
    scan_ms_res: float
    scan_ms_rl: float
    traced_bytes: int | None


@dataclasses.dataclass
class WindowStats:
    """Per-sweep-window denial-path latency aggregation (real wall time)."""

    t: float
    denials: int
    denied_mean_ms: float
    denied_max_ms: float
    opportunistic_scans: int


@dataclasses.dataclass
class SoakResult:
    peak_tracked_res: int
    peak_tracked_rl: int
    final_tracked_res: int
    final_tracked_rl: int
    denied_total: int
    admitted_total: int
    opportunistic_scans_res: int
    opportunistic_scans_rl: int
    at_cap_scan_ms_rl: float  # median scan cost while tracked ~ cap
    steady_scan_ms_rl: float  # median scan cost in the final (reclaimed) stretch
    sweep_samples: list[SweepSample]
    windows: list[WindowStats]


async def _run_soak(
    *,
    clock: VirtualClock,
    rl_ref: KeyedRateLimitRef,
    res_ref: KeyedReservationRef,
    settings: WorkerSettings,
    with_memory_trace: bool,
) -> SoakResult:
    reg = RateLimitRegistry()

    # Instrument (benchmark-side only): count the O(n) eviction scans that the
    # opportunistic (cap-pressure) path actually executes, by wrapping the
    # eviction methods on the instance. The per-worker sweep below calls the
    # captured originals directly, so the counters see ONLY opportunistic
    # scans — the gate lets through at most one per 30s if it holds.
    counters = {"res": 0, "rl": 0}
    orig_sweep_res = reg.evict_idle_keyed_reservations
    orig_sweep_rl = reg.evict_idle_keyed_rate_limits

    def counted_sweep_res(idle_for: timedelta) -> int:
        counters["res"] += 1
        return orig_sweep_res(idle_for)

    def counted_sweep_rl(idle_for: timedelta) -> int:
        counters["rl"] += 1
        return orig_sweep_rl(idle_for)

    reg.evict_idle_keyed_reservations = counted_sweep_res  # type: ignore[method-assign]
    reg.evict_idle_keyed_rate_limits = counted_sweep_rl  # type: ignore[method-assign]

    if with_memory_trace:
        tracemalloc.start()

    pool_rl = [f"pool-rl-{i:05d}" for i in range(POOL_SIZE)]
    pool_res = [f"pool-res-{i:05d}" for i in range(POOL_SIZE)]

    samples: list[SweepSample] = []
    windows: list[WindowStats] = []
    denied_latencies_ms: list[float] = []

    denied_total = 0
    admitted_total = 0
    peak_res = 0
    peak_rl = 0

    rl_arrivals_per_sec = STREAMING_RL_KEYS / STREAMING_PHASE_SECONDS
    res_arrivals_per_sec = STREAMING_RES_KEYS / STREAMING_PHASE_SECONDS
    rl_carry = 0.0
    res_carry = 0.0
    next_rl_idx = 0
    next_res_idx = 0

    perf = time.perf_counter

    async def admit_rl(name_key: str) -> bool:
        nonlocal admitted_total, denied_total
        t0 = perf()
        try:
            await reg._resolve_rate_limit_name(
                rl_ref,
                SoakPayload(tenant_id=name_key),
                settings=settings,
                pg_pool=None,
            )
        except ReservationUnavailable:
            denied_latencies_ms.append((perf() - t0) * 1000.0)
            denied_total += 1
            return False
        admitted_total += 1
        return True

    async def admit_res(name_key: str) -> bool:
        nonlocal admitted_total, denied_total
        t0 = perf()
        try:
            await reg._resolve_reservation_name(
                res_ref,
                SoakPayload(tenant_id=name_key),
                pg_pool=None,
                settings=settings,
            )
        except ReservationUnavailable:
            denied_latencies_ms.append((perf() - t0) * 1000.0)
            denied_total += 1
            return False
        admitted_total += 1
        return True

    window_denials = 0
    window_scans_start = (counters["res"], counters["rl"])

    while clock.now_virtual < TOTAL_VIRTUAL_SECONDS:
        clock.now_virtual += 1.0
        t = clock.now_virtual

        # --- worker activity at this virtual second --------------------------
        if t <= STREAMING_PHASE_SECONDS:
            rl_carry += rl_arrivals_per_sec
            while rl_carry >= 1.0 and next_rl_idx < STREAMING_RL_KEYS:
                rl_carry -= 1.0
                if not await admit_rl(f"t{next_rl_idx:07d}"):
                    window_denials += 1
                next_rl_idx += 1
            res_carry += res_arrivals_per_sec
            while res_carry >= 1.0 and next_res_idx < STREAMING_RES_KEYS:
                res_carry -= 1.0
                if not await admit_res(f"s{next_res_idx:07d}"):
                    window_denials += 1
                next_res_idx += 1

        # --- per-worker sweep every 30 s (both dicts, 1 h idle threshold) ----
        if t % SWEEP_CADENCE_SECONDS == 0.0:
            t0 = perf()
            evicted_res = orig_sweep_res(IDLE_THRESHOLD)
            scan_ms_res = (perf() - t0) * 1000.0
            t0 = perf()
            evicted_rl = orig_sweep_rl(IDLE_THRESHOLD)
            scan_ms_rl = (perf() - t0) * 1000.0

            tracked_res = len(reg._keyed_reservation_last_used)
            tracked_rl = len(reg._keyed_rate_limit_last_used)
            peak_res = max(peak_res, tracked_res)
            peak_rl = max(peak_rl, tracked_rl)

            traced_bytes: int | None = None
            if with_memory_trace:
                traced_bytes = tracemalloc.get_traced_memory()[0]
            samples.append(
                SweepSample(
                    t=t,
                    tracked_res=tracked_res,
                    tracked_rl=tracked_rl,
                    evicted_res=evicted_res,
                    evicted_rl=evicted_rl,
                    scan_ms_res=scan_ms_res,
                    scan_ms_rl=scan_ms_rl,
                    traced_bytes=traced_bytes,
                )
            )

            # --- recurring pools: keep these entries live ---------------------
            for key in pool_rl:
                await admit_rl(key)
            for key in pool_res:
                await admit_res(key)

            # --- close the denial-latency window ------------------------------
            window_scans_now = (counters["res"], counters["rl"])
            scans_in_window = (
                window_scans_now[0] - window_scans_start[0],
                window_scans_now[1] - window_scans_start[1],
            )
            if denied_latencies_ms:
                windows.append(
                    WindowStats(
                        t=t,
                        denials=len(denied_latencies_ms),
                        denied_mean_ms=statistics.fmean(denied_latencies_ms),
                        denied_max_ms=max(denied_latencies_ms),
                        opportunistic_scans=max(scans_in_window),
                    )
                )
                denied_latencies_ms.clear()
            window_denials = 0
            window_scans_start = window_scans_now

    final_res = len(reg._keyed_reservation_last_used)
    final_rl = len(reg._keyed_rate_limit_last_used)

    # Scan-cost trend: median cost while at cap (phase 1, size ~ CAP) vs the
    # final reclaimed stretch (size ~ POOL_SIZE).
    at_cap = [s.scan_ms_rl for s in samples if s.tracked_rl >= CAP * 0.95 and s.tracked_rl <= CAP]
    steady = [s.scan_ms_rl for s in samples if s.t >= 7500.0]

    return SoakResult(
        peak_tracked_res=peak_res,
        peak_tracked_rl=peak_rl,
        final_tracked_res=final_res,
        final_tracked_rl=final_rl,
        denied_total=denied_total,
        admitted_total=admitted_total,
        opportunistic_scans_res=counters["res"],
        opportunistic_scans_rl=counters["rl"],
        at_cap_scan_ms_rl=statistics.median(at_cap) if at_cap else float("nan"),
        steady_scan_ms_rl=statistics.median(steady) if steady else float("nan"),
        sweep_samples=samples,
        windows=windows,
    )


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _print_checkpoint_table(result: SoakResult, traced: bool) -> None:
    step = max(1, len(result.sweep_samples) // 15)
    picked = result.sweep_samples[::step]
    if not picked or picked[-1] is not result.sweep_samples[-1]:
        picked.append(result.sweep_samples[-1])

    header = (
        f"{'t_virt':>7} {'tracked_res':>11} {'tracked_rl':>10} "
        f"{'evict_res':>9} {'evict_rl':>8} {'scan_res_ms':>11} {'scan_rl_ms':>10}"
    )
    if traced:
        header += f" {'traced_MB':>10}"
    print(header)
    print("-" * len(header))
    for s in picked:
        line = (
            f"{s.t:>7.0f} {s.tracked_res:>11,} {s.tracked_rl:>10,} "
            f"{s.evicted_res:>9,} {s.evicted_rl:>8,} {s.scan_ms_res:>11.3f} {s.scan_ms_rl:>10.3f}"
        )
        if traced and s.traced_bytes is not None:
            line += f" {s.traced_bytes / 1e6:>10.2f}"
        print(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--memory",
        action="store_true",
        help="enable tracemalloc for the memory trend (adds overhead; use for RSS/trend numbers)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="optional path to write the full per-sweep sample CSV",
    )
    args = parser.parse_args(argv)

    clock = VirtualClock()
    # Benchmark-side virtual-time patch: every stamp/cutoff/gate inside the
    # registry module now reads the compressed clock. Patch the MODULE (via
    # sys.modules — see _registry_module), not the name-shadowed singleton.
    _registry_module().monotonic = clock  # type: ignore[assignment]

    settings = WorkerSettings.load()
    settings.max_keyed_rate_limits = CAP
    settings.max_keyed_reservations = CAP

    rl_ref, res_ref = _make_refs()

    t0 = time.perf_counter()
    result = asyncio.run(
        _run_soak(
            clock=clock,
            rl_ref=rl_ref,
            res_ref=res_ref,
            settings=settings,
            with_memory_trace=args.memory,
        )
    )
    real_s = time.perf_counter() - t0

    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS, KiB on Linux
    import sys

    rss_note = "ru_maxrss bytes (macOS)" if sys.platform == "darwin" else "ru_maxrss KiB (Linux)"

    total_virtual_min = int(TOTAL_VIRTUAL_SECONDS // 60)
    print()
    print(
        f"== soak_cardinality (virtual 2.5 h, streamed in {real_s:.1f} s real; "
        f"compression ~{TOTAL_VIRTUAL_SECONDS / real_s:,.0f}x; tracemalloc={'on' if args.memory else 'off'}) =="
    )
    print()
    _print_checkpoint_table(result, traced=args.memory)
    print()
    print("== summary ==")
    print(
        f"  distinct keys fed           : {_fmt_int(STREAMING_RL_KEYS + STREAMING_RES_KEYS)} "
        f"(rate-limits {STREAMING_RL_KEYS:,} / reservations {STREAMING_RES_KEYS:,}) "
        f"+ recurring pools {POOL_SIZE * 2}"
    )
    print(f"  acquisitions admitted/denied: {result.admitted_total:,} / {result.denied_total:,}")
    print(
        f"  peak tracked (res / rl)     : {_fmt_int(result.peak_tracked_res)} / "
        f"{_fmt_int(result.peak_tracked_rl)}  (caps {CAP:,} each)"
    )
    print(
        f"  final tracked (res / rl)    : {_fmt_int(result.final_tracked_res)} / "
        f"{_fmt_int(result.final_tracked_rl)}  (recurring pools {POOL_SIZE} each)"
    )
    print(
        f"  opportunistic scans (res/rl): {result.opportunistic_scans_res} / "
        f"{result.opportunistic_scans_rl} over {total_virtual_min} virtual min "
        f"(gate {OPPORTUNISTIC_GATE_SECONDS:.0f}s => max ~{int(TOTAL_VIRTUAL_SECONDS // 30)})"
    )
    print(
        f"  median eviction scan cost   : at cap {result.at_cap_scan_ms_rl:.3f} ms "
        f"(~{CAP:,} entries) vs steady {result.steady_scan_ms_rl:.3f} ms "
        f"(~{POOL_SIZE:,} entries)"
    )
    if result.windows:
        denial_means = [w.denied_mean_ms for w in result.windows if w.denials]
        scan_windows = [w for w in result.windows if w.opportunistic_scans]
        noscan_windows = [w for w in result.windows if not w.opportunistic_scans]
        if denial_means:
            print(
                f"  denial-path latency/window  : median of means "
                f"{statistics.median(denial_means):.4f} ms"
            )
        if scan_windows:
            print(
                f"    windows WITH gated scan   : mean-of-means "
                f"{statistics.fmean(w.denied_mean_ms for w in scan_windows):.4f} ms "
                f"({len(scan_windows)} windows)"
            )
        if noscan_windows:
            print(
                f"    windows without scan      : mean-of-means "
                f"{statistics.fmean(w.denied_mean_ms for w in noscan_windows):.4f} ms "
                f"({len(noscan_windows)} windows)"
            )
    if args.memory:
        print(f"  process RSS                 : {rss_kb:,} ({rss_note})")

    print()
    print("== verdict ==")
    checks = [
        (
            "bounded growth (peak <= cap)",
            result.peak_tracked_rl <= CAP and result.peak_tracked_res <= CAP,
            f"peak rl {result.peak_tracked_rl:,} vs {CAP:,}, "
            f"peak res {result.peak_tracked_res:,} vs {CAP:,}",
        ),
        (
            "30s gate holds (scans <= 1/gate interval while ~denials/s sustained)",
            result.opportunistic_scans_rl <= TOTAL_VIRTUAL_SECONDS / 30 + 2
            and result.opportunistic_scans_res <= TOTAL_VIRTUAL_SECONDS / 30 + 2,
            f"rl {result.opportunistic_scans_rl}, res {result.opportunistic_scans_res} scans "
            f"for {result.denied_total:,} denials",
        ),
        (
            "post-eviction steady state == live working set",
            result.final_tracked_rl <= POOL_SIZE * 2 and result.final_tracked_res <= POOL_SIZE * 2,
            f"final rl {result.final_tracked_rl:,}, res {result.final_tracked_res:,} "
            f"(pools {POOL_SIZE})",
        ),
        (
            "scan cost bounded & shrinking",
            result.steady_scan_ms_rl < result.at_cap_scan_ms_rl,
            f"at-cap {result.at_cap_scan_ms_rl:.3f} ms -> steady {result.steady_scan_ms_rl:.3f} ms",
        ),
    ]
    for name, ok, detail in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    if args.csv is not None:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with args.csv.open("w", encoding="utf-8") as fh:
            fh.write(
                "t_virtual,tracked_res,tracked_rl,evicted_res,evicted_rl,"
                "scan_ms_res,scan_ms_rl,traced_bytes\n"
            )
            for s in result.sweep_samples:
                fh.write(
                    f"{s.t},{s.tracked_res},{s.tracked_rl},{s.evicted_res},{s.evicted_rl},"
                    f"{s.scan_ms_res:.6f},{s.scan_ms_rl:.6f},"
                    f"{'' if s.traced_bytes is None else s.traced_bytes}\n"
                )
        print(f"\n  full per-sweep samples -> {args.csv}")

    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
