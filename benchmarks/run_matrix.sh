#!/usr/bin/env bash
# Cross-version benchmark matrix: run the A/B suite under Python 3.12/3.13/3.14.
#
# For each version, creates a uv venv under .bench-matrix/ (gitignored),
# installs taskq editable plus the profiling extras, runs
# benchmarks/run_bench.py --json, and collects every result JSON into
# benchmarks/results/matrix-<ts>/. Ends with a compact bench x version
# summary table of median A-side ns/op.
#
# Fail-soft: a version that cannot be built or benchmarked is skipped with a
# note; the script exits 0 if at least one version produced results.
#
# Usage:
#     bash benchmarks/run_matrix.sh                 # 3.12 3.13 3.14
#     BENCH_MATRIX_VERSIONS="3.13 3.14" bash benchmarks/run_matrix.sh
set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MATRIX_ROOT="$REPO_ROOT/.bench-matrix"
RESULTS_DIR="$SCRIPT_DIR/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$RESULTS_DIR/matrix-$STAMP"
VERSIONS="${BENCH_MATRIX_VERSIONS:-3.12 3.13 3.14}"

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv is required for the cross-version matrix and was not found on PATH." >&2
    echo "install it first: https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
fi

# Any stdlib python can build the summary table; prefer the repo venv if present.
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PY_SUM="$REPO_ROOT/.venv/bin/python"
elif [ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]; then
    PY_SUM="$REPO_ROOT/.venv/Scripts/python.exe"
else
    PY_SUM="$(command -v python3 || true)"
fi
if [ -z "${PY_SUM:-}" ]; then
    PY_SUM="$(uv run --no-project python 2>/dev/null || true)"
fi
if [ -z "${PY_SUM:-}" ]; then
    echo "error: no python3 found to build the summary table." >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
echo "matrix output: $OUT_DIR"

ok_versions=()
for ver in $VERSIONS; do
    tag="py${ver//./}"
    venv_dir="$MATRIX_ROOT/$tag"
    echo "── py$ver ──────────────────────────────────"
    if ! uv venv --python "$ver" "$venv_dir" 2>&1; then
        echo "  SKIP py$ver: uv venv --python $ver failed (interpreter unavailable?)" >&2
        continue
    fi
    if [ -x "$venv_dir/bin/python" ]; then
        PYBIN="$venv_dir/bin/python"
    elif [ -x "$venv_dir/Scripts/python.exe" ]; then
        PYBIN="$venv_dir/Scripts/python.exe"
    else
        echo "  SKIP py$ver: no interpreter at $venv_dir" >&2
        continue
    fi
    if ! uv pip install --python "$PYBIN" -e "$REPO_ROOT" py-spy pyinstrument; then
        echo "  SKIP py$ver: install failed" >&2
        continue
    fi
    echo "  running suite…"
    if "$PYBIN" "$SCRIPT_DIR/run_bench.py" --json > "$OUT_DIR/$tag.json"; then
        ok_versions+=("$tag")
        echo "  results: $OUT_DIR/$tag.json"
    else
        echo "  SKIP py$ver: run_bench.py failed" >&2
    fi
done

if [ "${#ok_versions[@]}" -eq 0 ]; then
    echo "error: no version produced results." >&2
    exit 1
fi

"$PY_SUM" - "$OUT_DIR" "${ok_versions[@]}" <<'EOF'
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
tags = sys.argv[2:]
by_tag: dict[str, dict[str, float]] = {}
names: list[str] = []
for tag in tags:
    doc = json.loads((out_dir / f"{tag}.json").read_text())
    by_tag[tag] = {r["name"]: r["a_ns"] for r in doc["results"]}
    for r in doc["results"]:
        if r["name"] not in names:
            names.append(r["name"])


def fmt(ns: float | None) -> str:
    if ns is None:
        return "-"
    if ns < 1e3:
        return f"{ns:,.0f} ns"
    if ns < 1e6:
        return f"{ns / 1e3:,.2f} us"
    return f"{ns / 1e6:,.2f} ms"


width = max((len(n) for n in names), default=5) + 2
col = 14
print()
print(f"{'bench (A-side median ns/op)':<{width}}" + "".join(f"{t:>{col}}" for t in tags))
print("-" * (width + col * len(tags)))
for name in names:
    row = f"{name:<{width}}"
    for tag in tags:
        row += f"{fmt(by_tag[tag].get(name)):>{col}}"
    print(row)
print()
EOF

echo "done: $(ls "$OUT_DIR" | wc -l | tr -d ' ') file(s) in $OUT_DIR"
