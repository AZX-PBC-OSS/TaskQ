# THE OPEN DEFECT — the rolling fleet's join starvation (the phase-5 gate's finding)

## The shape

`tests/system_e2e/test_rolling_release_fleet.py` (the VANILLA tier's
rolling-fleet file, pre-existing on main) is FLAKY-RED on the
consolidated branch: 2-3 of its 7 scenarios red per run, the failure
mode ALWAYS the same — `spawn_joined_worker`'s join-verification misses
its windows:

```
AssertionError: pods ['w1', 'w2', 'w3', 'w4'] never registered a worker
row within 30s
```

The helper's own remedy (reap + respawn, 3 attempts) exhausts.

## The controlled evidence

| Run | Tree | Result |
|---|---|---|
| the system-tier gate (the merged head, the shared box, 53 tests) | branch | 3 failed (this file) / 50 passed |
| the file solo (the merged head, the co-tenant's `-n 8` battery live) | branch | 2 failed / 5 passed |
| the file solo (the pre-merge head `17914536`, the fresh worktree) | branch | **21.4s to join, 2 worker rows for 4 LIVE pods** |
| the file (origin/main, the fresh worktree, the same hour) | main | **7/7 green, the boot 4.0s** |
| the file (origin/main, LATER the same hour, the load 3.6) | main | **7/7 green again** |
| a 4-pod plain spawn (the branch, no join-verification) | branch | all 4 rows in 1.0s |

**PRE-EXISTING on the branch** (red at `17914536`, before the merge) —
NOT a merge regression. The defect entered somewhere in the branch's
own chain.

## The narrowing (this round's instruments)

* The heartbeat's own tick cadence, instrumented (the temporary prints,
  since reverted): **ticks 3.5-3.8s apart, ALTERNATING with 0.5s ticks;
  the tick's own statements 0.00-0.02s** — the time goes into the
  LOOP'S WAIT, not the tick's work: `asyncio.sleep(0.49)` wakes ~3.3s
  late. THE EVENT LOOP IS STARVED, periodically, ~3.5s a pass.
* The watchdog's instruments see NOTHING: the lag warn budget is 5.0s
  (the 3.5s stalls duck under it), the stall-attribution tally stays
  EMPTY, the loop-idle window publishes nothing, no GIL/blocking
  attribution — the built instruments' thresholds sit ABOVE this
  defect's amplitude.
* The task dumps (SIGUSR2, 4x300ms): every task at its IDLE position
  (`Event.wait` / `Queue.get` / `sleep`) — the stall is in a SYNC
  callback (no task frame) or between dumps.
* The PG server: zero lock waits, zero blocking pids, all sessions
  idle — THE DATABASE IS NOT THE BOTTLENECK.
* The MIXED-TREE CONTROL: the branch's `heartbeat.py` running on
  MAIN'S tree showed a HEALTHY 0.51s cadence — the defect is NOT in
  the heartbeat's own code; it is an interaction with another branch
  file.
* The co-tenancy confound (named honestly): the box hosts OTHER agent
  sessions' batteries (a `-n 8` run observed live, the shared PG server
  at 96% CPU serving other sessions' module DBs) — the flake AMPLITUDE
  is load-dependent, but the CONTROL (main green / branch red in the
  same window) holds: the branch is genuinely sensitized.

## The candidate suspects (the bisect's head start)

1. **ea82ee36** — the workflow intercept + the boot projection + the
   capability leg (the boot's newest sibling tasks; the vanilla boot's
   tail grew).
2. **c933c8a8** (T21) — the ring-prune sweep arm (the leader's sweep
   pass grew).
3. **2a836bcb** (T08) — the status/progress rollup (the leader's
   metrics tick's grouped read).
4. The obs/_otel.py's wf gauge registration (the leader's metrics
   surface).

The bisect's red/green signal: the 4-pod `_spawn_joined_fleet` join
time (the reproducible probe: <5s green, >15s red) over
`main..17914536` — ~7 steps.

## The gates' disposition (this round)

* The system tier's gate: **50 passed / 3 failed** — the 3 are THIS
  defect's; the deploy matrix's six cells, the marches, the operator's
  walk, the upgrade path: ALL GREEN in the same run.
* The full default battery: **13,073 passed / 69 failed** (the rotating
  co-tenancy class, every sampled victim green solo — the phase-4
  clean-base control's band 59-74) + THE GATE'S REAL CATCH: the batch
  COPY's arity (the loop-budget trio in the COPY's column list but not
  the record — every fast-path batch dead by IndexError; CURED, 54
  passed).

## The cure's law (for the rev that lands it)

The join-verification helper's windows (BOOT_READY_BOUND_S / the 5s
heartbeat-advance check) are the tier's OWN derived bounds — they are
correct for a healthy boot; the defect is the BOOT'S TAIL. Find the
periodic ~3.5s blockage (the bisect above), fix the root, and the
helper's windows hold. Do NOT widen the helper's bounds to hide it.
