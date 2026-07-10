#!/usr/bin/env bash
# Runs each test file individually and records timings.
# Output: scripts/test_timings.txt

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$REPO_ROOT/scripts/test_timings.txt"

cd "$REPO_ROOT"

{
  printf "%-80s %10s\n" "TEST FILE" "TOTAL(s)"
  printf '%s\n' "$(printf '%.0s-' {1..92})"
} | tee "$OUT"

for f in tests/test_*.py; do
  start=$(date +%s%N)
  raw=$(uv run pytest "$f" --tb=no -q --no-header --durations=0 2>&1 || true)
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  elapsed_s=$(awk "BEGIN{printf \"%.2f\", $elapsed/1000}")

  # Extract individual test durations from --durations=0 output
  slowest=$(echo "$raw" | grep -E '^\s+[0-9]+\.[0-9]+ s ' | head -5 || true)

  printf "%-80s %10s\n" "$f" "${elapsed_s}s" | tee -a "$OUT"
  if [[ -n "$slowest" ]]; then
    echo "$slowest" | while IFS= read -r line; do
      printf "    %s\n" "$line"
    done | tee -a "$OUT"
  fi
done

echo "" | tee -a "$OUT"
echo "Done. Results saved to $OUT"
