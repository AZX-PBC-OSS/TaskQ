#!/usr/bin/env bash
# Contended-CPU test lane.
#
# Purpose: surface starvation-induced failures that an idle 32-core dev box
# hides. The test runner and every process it spawns (pytest-xdist workers,
# taskq worker subprocesses, asyncio loops) are pinned to one CPU core, and the
# suite's Postgres and Dragonfly containers are pinned to the same core. All
# deadline, budget, lease-renewal, heartbeat, and watchdog logic then runs with
# the CPU as the scarcest resource, which is the failure class CI never sees.
#
# Profile:
#   - test processes -> core $CORE (affinity is inherited by all children)
#   - test-spawned postgres:*/dragonfly* containers -> core $CORE (docker update)
#   - the docker-compose dev stack (taskq-*) is never touched
#
# Usage:
#   scripts/test-contended.sh [--core N] [--workers N] [--repeat N] [--profile same-core|split] [-- pytest args...]
#
#   --core N          CPU core to pin everything to (default: 0)
#   --workers N       pytest -n factor (default: 2; under 1-core contention
#                     higher factors multiply the starvation pressure)
#   --repeat N        run the selection N times and report per-run results
#                     (default: 1; use 3+ to separate flake from deterministic)
#   --profile split   workers on core $CORE, containers on core $((CORE+1))
#                     instead of both on $CORE (hyperthread-sibling severity)
#
# Requirements and honest limits:
#   - Run this lane SOLO. It pins every postgres/dragonfly container started
#     after lane startup; a concurrent testcontainers session would get its
#     containers pinned too. The script refuses to run if it sees foreign
#     postgres/dragonfly containers younger than its own start snapshot.
#   - The lane does not re-run full CI; it is a stress lens over the fast tier
#     (or any pytest selection you pass after --).
#   - A failure here is a robustness finding, not automatically a product bug:
#     triage each failure as (a) product logic that must hold under starvation,
#     (b) a test assertion that must be made starvation-robust, or (c) an
#     accepted CPU-boundary reality worth documenting.
set -euo pipefail

CORE=0
WORKERS=2
REPEAT=1
SPLIT=0
PYTEST_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --core) CORE="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --repeat) REPEAT="$2"; shift 2 ;;
    --profile) [ "$2" = "split" ] && SPLIT=1; shift 2 ;;
    --) shift; PYTEST_ARGS=("$@"); break ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

PG_CORE=$CORE
[ "$SPLIT" -eq 1 ] && PG_CORE=$((CORE + 1))

command -v docker >/dev/null || { echo "docker required" >&2; exit 1; }
command -v taskset >/dev/null || { echo "taskset required" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "docker daemon unreachable" >&2; exit 1; }

# Snapshot of container IDs alive before the lane starts. The poller pins only
# containers that appear after this point and match the suite's images, so the
# compose dev stack and pre-existing sessions are untouched.
KNOWN_IDS="$(docker ps --format '{{.ID}}' | sort)"

pin_new_containers() {
  local ids id
  ids="$(docker ps --format '{{.ID}} {{.Image}} {{.Names}}' \
    | grep -E 'postgres|dragonfly' \
    | grep -v ' taskq-' \
    | cut -d' ' -f1 | sort)"
  for id in $ids; do
    if ! grep -qx "$id" <<<"$KNOWN_IDS"; then
      docker update --cpuset-cpus="$PG_CORE" "$id" >/dev/null 2>&1 || true
    fi
  done
}

poller() {
  while :; do
    pin_new_containers
    sleep 2
  done
}

poller &
POLLER_PID=$!
trap 'kill "$POLLER_PID" 2>/dev/null || true' EXIT

if [ ${#PYTEST_ARGS[@]} -eq 0 ]; then
  PYTEST_ARGS=(-m "not integration")
fi

echo "== contended lane: workers on core $CORE, containers on core $PG_CORE,"
echo "== pytest: ${PYTEST_ARGS[*]}, workers=$WORKERS, repeat=$REPEAT"

fail=0
for i in $(seq 1 "$REPEAT"); do
  echo "---- repeat $i/$REPEAT ----"
  if taskset -c "$CORE" uv run pytest -n "$WORKERS" "${PYTEST_ARGS[@]}"; then
    echo "---- repeat $i: PASS ----"
  else
    echo "---- repeat $i: FAIL ----"
    fail=$((fail + 1))
  fi
done

echo "== contended lane summary: $fail failed of $REPEAT repeats"
exit "$fail"
