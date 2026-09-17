#!/usr/bin/env bash
# Contended-CPU soak lane over the repo's existing soak/benchmark engines.
#
# Pairs with scripts/test-contended.sh (that one stresses the pytest suite
# under a shared-core profile; this one stresses the standalone soak engines
# and reports throughput deltas).
#
# Engines (all pre-existing, no new soak logic here):
#   cpu          benchmarks/stress_dispatch.py — the per-job dispatch CPU path,
#                pure Python, no PG, prints jobs/sec.
#   cardinality  benchmarks/soak_cardinality.py — virtual-clock soak of the
#                keyed rate-limit registry, per-sweep samples to CSV; the CSV
#                is the leak/trend evidence (inspect it, do not eyeball the
#                console).
#
# For each engine the lane runs one unpinned baseline and then the contended
# profile (runner pinned to core $CORE), $REPEAT times each, and prints a
# delta table. A small contended/baseline ratio for the single-threaded cpu
# engine is expected (one core vs all cores); what the lane is really for is
# trend stability across repeats: a contended run that degrades repeat over
# repeat is a leak or an unbounded-queue finding, not a pinning artifact.
#
# Usage:
#   scripts/soak-contended.sh [--core N] [--repeat N] [--engine cpu|cardinality|all] [--csv-dir DIR]
#
#   --core N      core for the contended runs (default: 0)
#   --repeat N    runs per engine per profile (default: 3)
#   --csv-dir DIR where cardinality CSVs go (default: benchmarks/results/soak-contended)
#
# Honest limits:
#   - These engines are in-process; they do not exercise PG, network, or the
#     docker workers. For system-level contention combine this lane with
#     scripts/test-contended.sh and the e2e suite, and run the pg-spike probes
#     (benchmarks/multi_worker_spike.py) manually with a real DSN.
#   - Run solo: concurrent builds or test tiers on this machine show up
#     directly in the numbers.
#   - The lane reports measurements; it does not grade them. Compare against a
#     saved baseline (make bench-save) or against previous CSVs before calling
#     any delta a regression.
set -euo pipefail

CORE=0
REPEAT=3
ENGINE=all
CSV_DIR=benchmarks/results/soak-contended

while [ $# -gt 0 ]; do
  case "$1" in
    --core) CORE="$2"; shift 2 ;;
    --repeat) REPEAT="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --csv-dir) CSV_DIR="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

case "$ENGINE" in
  cpu|cardinality|all) ;;
  *) echo "unknown engine: $ENGINE" >&2; exit 2 ;;
esac

mkdir -p "$CSV_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

run_engine() {
  # $1 = label, $2 = core or "none", $3 = engine; echoes one summary number
  local label="$1" core="$2" engine="$3" out csv
  out="$(mktemp)"
  if [ "$engine" = cpu ]; then
    # stress_dispatch prints "jobs: N in Xs -> 1,234 jobs/sec" (comma-grouped)
    if [ "$core" = none ]; then
      uv run python benchmarks/stress_dispatch.py >"$out" 2>&1
    else
      taskset -c "$core" uv run python benchmarks/stress_dispatch.py >"$out" 2>&1
    fi
    grep 'jobs/sec' "$out" | sed 's/,//g' | grep -oE '[0-9]+(\.[0-9]+)? +jobs/sec' | tail -1 | grep -oE '^[0-9]+(\.[0-9]+)?'
  else
    # CSV columns: t_virtual,tracked_res,tracked_rl,evicted_res,evicted_rl,
    #              scan_ms_res,scan_ms_rl,traced_bytes
    # The soak's leak evidence is the tracked_res trend (first vs last sample);
    # report both plus the final scan latency so the delta line shows growth.
    csv="$CSV_DIR/cardinality-$label-$STAMP.csv"
    if [ "$core" = none ]; then
      uv run python benchmarks/soak_cardinality.py --memory --csv "$csv" >"$out" 2>&1
    else
      taskset -c "$core" uv run python benchmarks/soak_cardinality.py --memory --csv "$csv" >"$out" 2>&1
    fi
    echo "first_tracked=$(sed -n '2p' "$csv" | cut -d, -f2) last_tracked=$(tail -1 "$csv" | cut -d, -f2) last_scan_ms_rl=$(tail -1 "$csv" | cut -d, -f7)"
    grep '\[FAIL\]' "$out" || true
  fi
  rm -f "$out"
}

declare -A BASE CONT
for eng in cpu cardinality; do
  [ "$ENGINE" != all ] && [ "$ENGINE" != "$eng" ] && continue
  for _ in $(seq 1 "$REPEAT"); do
    BASE["$eng"]="${BASE["$eng"]:-}|$(run_engine base none "$eng")"
    CONT["$eng"]="${CONT["$eng"]:-}|$(run_engine contended "$CORE" "$eng")"
  done
  echo "== $eng"
  echo "   baseline:  ${BASE["$eng"]#|}"
  echo "   contended: ${CONT["$eng"]#|}"
  if [ "$eng" = cardinality ]; then
    echo "   CSVs in $CSV_DIR: first vs last tracked_res is the leak evidence; a"
    echo "   contended series that grows repeat over repeat is a finding, not noise."
  fi
done
