#!/bin/sh
# Runtime bake-off: Node vs Bun vs Deno as TaskQ's TS actor runtime.
# Prereqs: node, bun, deno (npm install -g deno), python3.
set -e
cd "$(dirname "$0")"

[ -d node_modules ] || npm install --no-fund --no-audit

python3 bench_driver.py versions
python3 bench_driver.py warm      # warm RTT + memory (idle -> 1000 jobs)
python3 bench_driver.py mux       # 1 process x 20 conc and 4 processes x 5 conc
python3 bench_driver.py startup   # cold spawn -> handshake -> first RTT, 200 trials
python3 bench_driver.py kill      # SIGKILL reap time, SIGTERM on stdin block, stdin EOF
python3 bench_driver.py sandbox   # capability-model probes (node --permission / deno perms / bun)

echo "done — results in results/"
