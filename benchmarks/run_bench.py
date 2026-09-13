"""CLI runner around benchmarks/bench_hotspots.py — structured results, baselines, regression gate.

Wraps the A/B harness (imported directly, so results stay structured instead of
parsed from stdout) and adds benchmark-infrastructure plumbing:

    python benchmarks/run_bench.py                    # full suite, human table
    python benchmarks/run_bench.py --only jsonb,decode
    python benchmarks/run_bench.py --stall            # + event-loop stall probes
    python benchmarks/run_bench.py --json             # machine-readable stdout
    python benchmarks/run_bench.py --save-baseline    # write canonical baseline
    python benchmarks/run_bench.py --check            # gate vs baseline.json
    python benchmarks/run_bench.py --check other.json --threshold 1.25

Every run that executes benches also appends a history file
``benchmarks/results/run-<iso-ts>-<shortsha>.json`` (gitignored).

Exit codes for ``--check``: 0 clean, 1 regression or correctness mismatch,
2 missing or unusable baseline.

Pure stdlib, cross-platform (macOS/Linux/Windows), Python 3.12+.

Regression rule (deliberate, documented — the gate compares the ratio of
medians of interleaved A batches, which already cancels thermal/frequency
drift within a run):

    REGRESSION  current.a_ns / baseline.a_ns > threshold   (default 1.20)
                AND current.a_ns - baseline.a_ns > --min-abs-delta (default 100 ns)

    IMPROVED    symmetric: ratio < 1/threshold AND the absolute drop exceeds
                --min-abs-delta.

Requiring BOTH the relative and the absolute condition keeps benches whose
whole magnitude sits inside timer/jitter noise (sub-microsecond) from
flip-flopping the gate on machine noise; the relative condition alone would
flag a 300ns -> 305ns drift on a noisy box, the absolute alone would never
fire for large benches. ``correct=False`` on the current run is reported as
MISMATCH and also fails the gate — a variant that stopped being
output-identical is a worse regression than a slow one.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import platform
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import bench_hotspots as bh

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, str(_BENCH_DIR))

# bench_hotspots (and therefore taskq) is imported lazily, inside the bench
# execution path, so this module stays loadable for --check bookkeeping and
# for testing compare() without a working taskq import.

#: The full A/B suite, mirroring bench_hotspots.main()'s dispatch exactly.
BENCH_GROUP_NAMES = ("di", "jsonb", "decode", "cron", "rows", "retry", "evict")


def _bench_groups() -> dict[str, Any]:
    import bench_hotspots as bh  # needs _BENCH_DIR on sys.path

    # Mirrors bench_hotspots.main(): pin the log config the production
    # dispatch path runs under so the DI solver's per-dep debug logging is
    # dropped before terminal rendering cost lands inside timed regions.
    from taskq.obs._structlog import setup_logging

    setup_logging(level="INFO", log_format="json")
    return {
        "di": lambda: [bh.bench_di_solver(), bh.bench_di_introspection_only()],
        "jsonb": bh.bench_jsonb,
        "decode": lambda: [*bh.bench_decode_jsonb(), *bh.bench_encode_json_stdlib_vs_orjson()],
        "cron": bh.bench_cron,
        "rows": bh.bench_job_row_decode,
        "retry": bh.bench_retry_policy,
        "evict": bh.bench_evict_keyed,
    }


# run_stall_probes() prints; we capture and parse rather than re-implement the
# probes, so the runner always measures exactly what the harness measures.
_STALL_LINE = re.compile(
    r"^\s*(?P<name>.+?)\s+max_stall=\s*(?P<max>[\d.]+)\s+ms"
    r"\s+mean=\s*(?P<mean>[\d.]+)\s+ms"
    r"\s+wall=\s*(?P<wall>[\d.]+)s\s+ticks=(?P<ticks>\d+)$"
)


# ── Environment metadata (portable, fail-soft) ────────────────────────


def _run(cmd: list[str]) -> str:
    try:
        # S603: cmd is a fixed git query built in this module — no untrusted input.
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603
    except OSError:
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def git_metadata() -> tuple[str, bool]:
    sha = _run(["git", "rev-parse", "HEAD"])
    dirty = bool(_run(["git", "status", "--porcelain"]))
    return sha, dirty


def cpu_brand() -> str:
    """CPU model string via the cheapest probe per OS; never fails."""
    if sys.platform == "darwin":
        brand = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        if brand:
            return brand
    if sys.platform.startswith("linux"):
        try:
            for line in (
                Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines()
            ):
                if line.startswith("model name") and ":" in line:
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
    # Windows (and any other OS): often empty or generic, but harmless.
    return platform.processor() or platform.machine() or "unknown"


# ── Suite execution ───────────────────────────────────────────────────


def run_benches(only: str) -> list[bh.ABResult]:
    groups = _bench_groups()
    wanted = {s.strip() for s in only.split(",") if s.strip()}
    unknown = wanted - set(groups)
    if unknown:
        raise SystemExit(
            f"unknown bench(es): {', '.join(sorted(unknown))}; choices: {', '.join(BENCH_GROUP_NAMES)}"
        )
    results: list[bh.ABResult] = []
    for name, fn in groups.items():
        if name in wanted or not wanted:
            results.extend(fn())
    return results


def run_stall_probes_captured() -> list[dict[str, Any]]:
    import bench_hotspots as bh  # lazy, needs _BENCH_DIR on sys.path

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        bh.run_stall_probes()
    probes: list[dict[str, Any]] = []
    for line in buf.getvalue().splitlines():
        m = _STALL_LINE.match(line)
        if m:
            probes.append(
                {
                    "name": m["name"],
                    "max_stall_ms": float(m["max"]),
                    "mean_stall_ms": float(m["mean"]),
                    "wall_s": float(m["wall"]),
                    "ticks": int(m["ticks"]),
                }
            )
    return probes


def collect(only: str, stall: bool) -> dict[str, Any]:
    sha, dirty = git_metadata()
    doc: dict[str, Any] = {
        "schema": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "git_sha": sha,
        "git_dirty": dirty,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cpu_brand": cpu_brand(),
        "results": [],
        "stall_probes": [],
    }
    for r in run_benches(only):
        doc["results"].append(
            {
                "name": r.name,
                "a_ns": r.a_ns_per_op,
                "b_ns": r.b_ns_per_op,
                "speedup": r.speedup,
                "correct": r.correct,
                "note": r.note,
            }
        )
    if stall:
        doc["stall_probes"] = run_stall_probes_captured()
    return doc


def write_history(doc: dict[str, Any]) -> Path:
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    gitignore = _RESULTS_DIR / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(
            "# Everything here is throwaway history or profiling output except\n"
            "# the canonical baseline, which is the tracked regression-gate input.\n"
            "*\n"
            "!.gitignore\n"
            "!baseline.json\n",
            encoding="utf-8",
        )
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    sha = str(doc.get("git_sha") or "nogit")[:7]
    path = _RESULTS_DIR / f"run-{ts}-{sha}.json"
    path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return path


# ── Comparison ────────────────────────────────────────────────────────


def compare(
    current: dict[str, Any],
    baseline: dict[str, Any],
    threshold: float,
    min_abs_delta_ns: float,
) -> tuple[list[dict[str, Any]], bool]:
    """Merge on bench name; returns (rows, failed). See module docstring for the rule.

    Verdicts that never fail the gate: NEW (bench absent from the baseline)
    and MISSING (bench absent from the current run). NEW is inevitable for a
    freshly added bench; MISSING keeps ``--only`` subset checks viable. A
    full run that dropped a bench therefore passes the gate with a MISSING
    row in the table — the suite itself is code-owned, so that shows up in
    review rather than in the exit code.
    """
    base_by_name = {r["name"]: r for r in baseline.get("results", [])}
    rows: list[dict[str, Any]] = []
    failed = False
    for cur in current.get("results", []):
        name = cur["name"]
        row: dict[str, Any] = {
            "name": name,
            "baseline_ns": None,
            "current_ns": cur["a_ns"],
            "delta_pct": None,
            "verdict": "NEW",
        }
        base = base_by_name.pop(name, None)
        if base is None:
            rows.append(row)
            continue
        row["baseline_ns"] = base["a_ns"]
        if base["a_ns"] > 0 and cur["a_ns"] > 0 and cur["correct"] and base.get("correct", True):
            ratio = cur["a_ns"] / base["a_ns"]
            abs_delta = cur["a_ns"] - base["a_ns"]
            row["delta_pct"] = (ratio - 1) * 100
            if ratio > threshold and abs_delta > min_abs_delta_ns:
                row["verdict"] = "REGRESSION"
                failed = True
            elif ratio < 1 / threshold and -abs_delta > min_abs_delta_ns:
                row["verdict"] = "IMPROVED"
            else:
                row["verdict"] = "OK"
        else:
            row["verdict"] = "MISMATCH"
            failed = True
        rows.append(row)
    for leftover in base_by_name.values():
        rows.append(
            {
                "name": leftover["name"],
                "baseline_ns": leftover["a_ns"],
                "current_ns": None,
                "delta_pct": None,
                "verdict": "MISSING",
            }
        )
    return rows, failed


def _fmt_ns(ns: float | None) -> str:
    if ns is None:
        return "-"
    if ns < 1e3:
        return f"{ns:,.0f} ns"
    if ns < 1e6:
        return f"{ns / 1e3:,.2f} us"
    return f"{ns / 1e6:,.2f} ms"


def print_check_table(
    rows: list[dict[str, Any]], threshold: float, min_abs_delta_ns: float
) -> None:
    print(f"\n{'bench':<44} {'baseline':>12} {'current':>12} {'delta':>8}  verdict")
    print("-" * 92)
    for r in sorted(rows, key=lambda x: (x["verdict"] != "REGRESSION", x["name"])):
        delta = f"{r['delta_pct']:>7.1f}%" if r["delta_pct"] is not None else f"{'-':>8}"
        print(
            f"{r['name']:<44} {_fmt_ns(r['baseline_ns']):>12} {_fmt_ns(r['current_ns']):>12} "
            f"{delta}  {r['verdict']}"
        )
    print(
        f"\nrule: REGRESSION if current/baseline > {threshold:.2f} AND slower by more than"
        f" {min_abs_delta_ns:,.0f} ns (medians of interleaved batches, both conditions required)"
    )


def print_run_table(doc: dict[str, Any]) -> None:
    results = doc["results"]
    print(f"\nPython {doc['python_version']} — A/B results (interleaved, median ns/op)")
    print(f"{'bench':<44} {'A (current)':>14} {'B (variant)':>14} {'speedup':>9}  ok")
    print("-" * 92)
    for r in results:
        a = _fmt_ns(r["a_ns"])
        b = _fmt_ns(r["b_ns"])
        flag = "OK" if r["correct"] else "MISMATCH!"
        note = f"  {r['note']}" if r["note"] else ""
        print(f"{r['name']:<44} {a:>14} {b:>14} {r['speedup']:>8.2f}x  {flag}{note}")
    for p in doc.get("stall_probes", []):
        print(
            f"stall: {p['name']:<36} max={p['max_stall_ms']:8.2f} ms  mean={p['mean_stall_ms']:6.2f} ms"
            f"  wall={p['wall_s']:5.2f}s  ticks={p['ticks']}"
        )


# ── main ──────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="TaskQ benchmark runner: structured A/B results, baselines, regression gate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--only", default="", help="comma-separated subset: di,jsonb,decode,cron,rows,retry,evict"
    )
    parser.add_argument("--stall", action="store_true", help="also run event-loop stall probes")
    parser.add_argument(
        "--json", action="store_true", help="print the structured result document to stdout"
    )
    parser.add_argument(
        "--save-baseline", action="store_true", help="save run as the canonical baseline"
    )
    parser.add_argument(
        "--check",
        nargs="?",
        const="",
        default=None,
        metavar="BASELINE",
        help="compare against a baseline (default: benchmarks/results/baseline.json); exit 1 on regression",
    )
    parser.add_argument(
        "--threshold", type=float, default=1.20, help="regression ratio threshold (default 1.20)"
    )
    parser.add_argument(
        "--min-abs-delta",
        type=float,
        default=100.0,
        help="minimum absolute ns/op slowdown to count as regression (default 100)",
    )
    args = parser.parse_args()

    # Bench functions should not print, and we keep stdout clean for --json;
    # capture anything that slips through and re-route it to stderr.
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        doc = collect(args.only, args.stall)

    captured = buf.getvalue()
    if captured.strip():
        print(captured, file=sys.stderr, end="")

    history_path = write_history(doc)

    if args.json:
        print(json.dumps(doc, indent=2))
    else:
        print_run_table(doc)
        print(f"\nhistory: {history_path}")

    if args.save_baseline:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        pyver = f"py{sys.version_info.major}{sys.version_info.minor}"
        sha = str(doc["git_sha"])[:7] or "nogit"
        versioned = _RESULTS_DIR / f"baseline-{pyver}-{sha}.json"
        canonical = _RESULTS_DIR / "baseline.json"
        for path in (versioned, canonical):
            path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
        if not args.json:
            print(f"baseline: {canonical} (copy: {versioned.name})")

    if args.check is not None:
        baseline_path = Path(args.check) if args.check else _RESULTS_DIR / "baseline.json"
        if not baseline_path.is_file():
            print(f"error: baseline not found: {baseline_path}", file=sys.stderr)
            print(
                "save one first: make bench-save (or python benchmarks/run_bench.py --save-baseline)",
                file=sys.stderr,
            )
            return 2
        try:
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            rows, failed = compare(doc, baseline, args.threshold, args.min_abs_delta)
            if not args.json:
                # Inside the guard too: a baseline with a non-numeric a_ns on a
                # MISSING row (name never merged) surfaces as a TypeError in the
                # table renderer, not in compare.
                print_check_table(rows, args.threshold, args.min_abs_delta)
                print(f"baseline: {baseline_path}")
        except (ValueError, KeyError, TypeError) as exc:
            # ValueError covers JSONDecodeError (and UnicodeDecodeError): a
            # corrupt or wrong-shaped baseline cannot be distinguished from
            # a missing one, so it takes the same exit code instead of an
            # uncaught traceback whose accidental exit 1 would masquerade
            # as a regression verdict.
            print(f"error: baseline unusable: {baseline_path}: {exc}", file=sys.stderr)
            return 2
        return 1 if failed else 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
