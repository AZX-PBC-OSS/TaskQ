# The debugging playbook: asyncio failures, test flakes, leaks, and hangs

Maintainer-facing playbook for the failure classes this suite actually
produces. Every section is a real incident from the week of 2026-09-14
through 2026-09-21, named by commit, and every command and code snippet
below was executed while writing this document: the output shapes are
real, not idealized.

Related docs: [sql-hotpath-followups.md](sql-hotpath-followups.md) (the
same method applied to query plans), the
[testing guide](../guides/testing.md) (lane and marker doctrine),
[troubleshooting](../guides/troubleshooting.md) (operator-facing
symptoms).

---

## 1. The method

Every investigation this week followed the same chain. Keep the chain
intact; each hop is evidence for the next.

1. **Symptom**: a failure, a timeout, a warning, a red CI lane. Write
   down the exact text.
2. **Capture**: get the dump while the state exists (task stacks, thread
   stacks, plan output, metric series). A symptom you cannot capture is
   a symptom you cannot attribute.
3. **Mechanism**: name the machinery that produced the captured state
   (module-scoped event loops, a daemon sampling thread, a server-side
   GUC, a scheduler decision).
4. **Root cause**: the line of code whose edit removes the mechanism.
   Not the test that flaked, not the runner that was slow.
5. **Deterministic repro**: a test that fails on the root cause alone,
   every run, no scheduler cooperation required.
6. **Fix**: the smallest change that makes the repro pass.
7. **Red/green**: the repro is red on the unfixed tree, green after.
   Both halves are required; a repro that was never red proves nothing.
8. **Sharpness (mutation) proof**: inject the defect the gate claims to
   catch and show the gate goes red. A gate that cannot fail is
   decoration.

Rules that decided arguments this week:

- **Reruns are not evidence.** "It passed on retry" is a coin landing
  heads, not a proof the coin is fair. The four lottery tests in
  `f012a275` passed many runs between failures; the fix commit records
  the honest metric, 10 of 10 runs, after the test stopped depending on
  timing, and never before.
- **"Environmental" requires proof by execution.** A claim that a
  failure is runner noise means demonstrating the noise: the 300s hang
  in `7607beb4` was called environmental until the hang-dump showed a
  task parked on an await with no timer pending, which proved the
  environment was innocent and the setup connection was unbounded.
- **Load lotteries are defects.** Any assertion whose outcome depends on
  co-tenant CPU, cache temperature, or xdist sibling scheduling is a
  bug in the test, whatever the product code is doing. Examples below:
  the 1.61x/doubling growth flake (`f896d3ec`), the 3.294ms "sub-ms"
  plan gate (`3d4cffca`), the 1ms statement timeout that raced its own
  claim (`f012a275`).

---

## 2. Reading a task dump

### The tool

`dump_task_stacks` in `src/taskq/worker/_watchdog.py` emits one
structured record per live asyncio task: name, coroutine qualifier, and
await-site frames (innermost first). It is the single implementation
behind four surfaces: the watchdog trip dump, the SIGUSR2 on-demand
handler (`src/taskq/worker/shutdown.py`), the `/tasks` health endpoint,
and the straggler logger. It also prints a raw stderr fallback so the
bundle survives a broken logging pipeline.

Executed shape (a demo driver creating three named tasks and two
anonymous ones, parked on `Event.wait` and `Queue.get`):

```
=== task dump (doc-demo/event-loop-lag-warn): 6 live task(s) ===
--- soak-worker-0 main.<locals>.park_forever @ /tmp/_doc_dump_demo.py:10
--- leader.sweep main.<locals>.park_forever @ /tmp/_doc_dump_demo.py:10
--- Task-2 main.<locals>.park_forever @ /tmp/_doc_dump_demo.py:10
--- worker.reload_coordinator main.<locals>.drain_queue @ /tmp/_doc_dump_demo.py:13
--- Task-3 main.<locals>.drain_queue @ /tmp/_doc_dump_demo.py:13
--- Task-1 main @ .../asyncio/runners.py:196, ...
```

The structured record the log pipeline receives:

```json
{"task": "soak-worker-0", "coro": "main.<locals>.park_forever",
 "sites": ["/tmp/_doc_dump_demo.py:10"]}
```

Read one line as: **who** (the task name), **what** (the coroutine
qualifier), **where** (the file:line it is suspended at). The `sites`
list is what you grep: a task parked at `asyncio/locks.py:213` is inside
`Event.wait`; a task parked at a queue module line is inside `get`.

### The incident

A lost-job soak test (`tests/test_rt_lost_job_soak.py`) bootstraps a
full worker: leader loops, sweeps, cron, notify listener, consumers,
heartbeat. When one of its teardown paths skipped cancel-and-await, the
module-scoped loop (the suite runs `asyncio_default_test_loop_scope =
"module"`, `pyproject.toml`) kept roughly forty of those tasks alive
into the NEXT test module. Their dumps showed the leak signature:

- most entries parked in `Event.wait` and `Queue.get`, inside bootstrap
  internals;
- the task names belonged to a DIFFERENT test's machinery
  (`leader.*`, `worker.*`, `soak-worker-*`): another test's named tasks
  appearing in your dump is the leak fingerprint;
- the counts matched the bootstrap's shape, not this test's, which is
  how you know the tasks are inherited and not spawned here.

### Telling leaked bootstrap tasks from legitimate ones

The worker names every task it creates. The prefixes are the registry
you diff against (`src/taskq/worker/leader.py`,
`src/taskq/worker/_bootstrap.py`, `src/taskq/worker/_watchdog.py`):

| Prefix | Owner | Examples |
|---|---|---|
| `leader.` | the leader's TaskGroup children | `leader.election`, `leader.watchdog`, `leader.cron`, `leader.sweep`, `leader.prune`, `leader.archive_expiry`, `leader.queue_depth` |
| `worker.` | worker-level services | `worker.shutdown_watchdog`, `worker.reload_coordinator` |
| `soak-worker-*` | test-minted worker tasks | `soak-worker-0` (`tests/test_rt_lost_job_soak.py:400`) |

A live worker mid-test legitimately holds a full `leader.*` family. A
leak is one of:

- a name from another test's worker or a `soak-worker-*` name when you
  run no soak;
- `Task-N`, an anonymous task: production code names everything it
  creates, so an anonymous `Task-N` in a dump is a `create_task` call
  that skipped the `name=` contract and is a defect to fix at the call
  site, not noise to ignore;
- a `leader.*` family in a module that never started a worker.

Direction of the read matters: the dump is attributed to the test that
CAPTURED it, but the defect lives in the test that CREATED the task.
That is the rule the leftover-task guard (section 6) enforces
mechanically.

---

## 3. Hangs

### The signature, and what it proves

The suite-wide pytest-timeout is 300s (`pyproject.toml` addopts,
`--timeout=300`; a real CI run prints `timeout: 300.0s` in its header).
When it fires with the main thread here:

```
fd_event_list = self._selector.poll(timeout, max_ev)
Failed: Timeout (>300.0s) from pytest-timeout.
```

(above run at the 2s mark in a scratch demo; the shape is identical),
it proves something precise: **the event loop had no timer pending.**
`epoll(-1)`-shaped parks are what a selector does when its timeout is
None, and the selector timeout is None when nothing on the loop needs
waking. Every `wait_for`, `sleep`, and command timeout would show up as
a scheduled timer. So the hung coroutine is sitting on an await that
nothing in the program will ever resolve: not "slow", not "starved",
but structurally unresolvable. That is the difference between a budget
problem (bump the timeout) and a mechanism problem (bound the await).

### The capture tooling

Two layers, both cheap to keep around:

1. **`PYTHONFAULTHANDLER=1`** (or `faulthandler.enable()`): on any hard
   crash or `faulthandler.dump_traceback()` call you get all thread
   stacks, including the loop thread parked in the selector.
2. **The hang-dump pytest plugin**: pytest-timeout's SIGALRM handler is
   a hook point. Chain it, dump every live task with
   `task.get_stack()` / `task.print_stack()`, then let the original
   handler raise. The chain matters: a dump AFTER the timeout has
   already unwound finds no running loop (executed and confirmed: a
   post-hoc dump prints `<no running loop on this thread>`), so the
   dump must run AT the hang instant. The executed source shape:

```python
@pytest.hookimpl(wrapper=True)
def pytest_runtest_call():
    original = signal.getsignal(signal.SIGALRM)

    def dump_then_raise(signum, frame):
        print("\n=== hang dump: pytest-timeout fired, live loop tasks ===")
        for line in _dump_live_tasks():
            print(line)
        original(signum, frame)  # pytest-timeout raises Failed here

    signal.signal(signal.SIGALRM, dump_then_raise)
    try:
        result = yield
        return result
    finally:
        signal.signal(signal.SIGALRM, original)
```

Executed against a test hung on an unset `Event`:

```
=== hang dump: pytest-timeout fired, live loop tasks ===
task 'Task-1' coro=test_hangs_on_unset_event
  /tmp/hangdemo/test_hangs.py:4 in test_hangs_on_unset_event
```

The same output shape arrived at by an in-process dump at the park
point, showing the timer evidence (one task parked at
`asyncio/locks.py:213`, zero scheduled timers):

```
--- hung.setup_conn Event.wait @ .../asyncio/locks.py:213
pending timers on the loop: 0
```

That pairing (task parked, no timers) is the whole diagnosis. It is the
tool the claim-epoch investigation (`59a47582`, the smallint ceiling
fence) used to sort a real deadlock suspicion from an unbounded wait.

### Worked example: the unbounded setup connection

Incident `7607beb4`: a CI hang in an OTel integration test, over 300s,
pytest-timeout fired, main thread in the selector with no timer
pending.

- Symptom: `Failed: Timeout > 300s` in the test (3.13) leg.
- Capture: the dump showed the test coroutine still pending on an idle
  module loop parked in epoll, no timer active.
- Mechanism: every await in the test's own path was bounded except one.
  `_setup_worker`'s raw `asyncpg.connect` ran the schema drop, the
  migration run, and the actor-config insert with no server-side
  `statement_timeout` or `lock_timeout`. asyncpg's connect timeout
  covers only the handshake.
- Root cause: the setup connection was the one unbounded await surface,
  so a stalled shared test container under `-n 4` suspended the test
  forever on an in-flight query.
- Fix: an explicit connect timeout plus session-level
  `statement_timeout`/`lock_timeout` on the setup connection. A stalled
  server now fails the test in seconds with a typed asyncpg
  `TimeoutError` naming the phase.

Corollary incident, the other direction: `e49fdad6` raised the
lost-job soak's pytest-timeout to 900 (the e2e family's existing value
for the same wall-clock-bound tier) because the soak's OWN bounds (240
paced rounds plus a quiescence cap near 4s per job) legitimately exceed
300s on a starved runner. A timeout kill mid-asyncio leaves tasks
pending on the module loop, which then surface as teardown ERRORs and
cross-test pollution. Distinguish the two shapes before touching a
timeout: unbounded await (fix the await) versus legitimate worst case
exceeding the backstop (raise the backstop for that tier).

---

## 4. Event-loop lag

### The warning

The loop-lag watchdog (`LoopLagWatchdog`, detector 4 in
`src/taskq/worker/_watchdog.py`) is a daemon thread that asks the loop
to run a beat callback and measures how late the beat lands. When the
gap crosses the warn budget it emits, once per stall:

- `worker-watchdog-lag-warn` with `lag_seconds` and `warn_budget`; and
- `event-loop-stall-attributed` with the stall's diagnosis:

```
event='event-loop-stall-attributed'
actor=<actor name or None>
job_id=<the one running job of that actor, when unique>
frame=<file:line:function, innermost non-taskq frame>
kind=blocking_call | gil_held
lag_seconds=<rounded stall age>
remedy=<one-line operator remedy>
```

The two kinds are the two mechanisms (see
`src/taskq/worker/_stall_tally.py`):

- `blocking_call`: the blocked frame released the GIL (I/O wait,
  `time.sleep`, a subprocess). The watchdog thread kept ticking.
  Remedy: move the call off the loop (`asyncio.to_thread` /
  `run_in_executor`) or make it async.
- `gil_held`: the interpreter is held (a C extension without a GIL
  release, a hot pure-Python loop). The watchdog thread's own wakeup
  gap overshoots, which is the second classifier signal. Remedy: chunk
  the work or move it off the loop.

The attribution is end-to-end tested against the real loop in
`tests/test_loop_stall_attribution.py`: a `time.sleep` probe for
`blocking_call`, a large `orjson.loads` probe for `gil_held`.

### Cause or symptom?

Read the warning as a measurement, not a verdict. The same signal sits
at two places in a causal chain:

- **Cause**: an actor's body genuinely blocks the loop (the
  `blocking_call` / `gil_held` cases above). The attributed frame names
  the code to change.
- **Symptom**: the loop is slow because something ELSE starved it, and
  the lag warning is merely where the sickness becomes visible.

The symptom case was the week's isolate-test incident: recurring
`TimeoutError`s in the heartbeat isolate tests resolved to a leaked
soak bootstrap (the ~40 `Event.wait`/`Queue.get` tasks of section 2)
advancing on the same module-scoped loop. Every await in the isolate
test took longer than designed because dozens of zombie tasks were
served at each other's await points; the lag warnings showed elevated
`lag_seconds` with no blocking frame to name. The lag warning pointed
at the loop; the task dump pointed at the leak; the leak was the root
cause.

### The dump pairing

The watchdog pairs the two signals by construction: a tier-1 warn
schedules `dump_task_stacks("loop-lag-recovered", ...)` to run on the
loop once it recovers (`asyncio.all_tasks` is not thread-safe, so the
thread cannot take the dump itself; it uses
`call_soon_threadsafe`). So in any log where a stall was attributed you
should find a `worker-task-dump` close behind it. Read them together:

- stall attributed + dump shows one task parked in a sync-looking
  frame: the stall is the CAUSE, the frame is the bug.
- stall attributed + dump shows dozens of foreign tasks: the stall is a
  SYMPTOM; hunt the leak (section 6).

The terminal tier (crossing the trip budget) force-exits the worker
after dumping thread and task stacks; in-flight jobs are reclaimed by
the leader sweep on lease expiry, which is why the trip must land
inside the lease (`WorkerSettings.post_load`'s lag-lease invariant).

---

## 5. Timing-scaling tests

### Why growth-ratio gates fail on shared runners

A growth-ratio gate times a workload at size N and at 8N and asserts
the ratio stays near the complexity's expectation. On a shared runner
the ratio is a lottery even when the code is honest:

- **Cache effects**: the small payload is cache-resident; the large one
  is not. The measured ratio therefore contains a memory-hierarchy
  constant on top of the algorithmic one, and the constant differs per
  runner and per minute.
- **Sample contamination**: a single timed round is the sum of the
  work plus every co-tenant interruption during it. Taking one round
  per size means the ratio of two contaminations.

Incident `f896d3ec`: the NUL-scan pin timed a payload of LIVE escapes,
which `_encoded_has_nul` answers at the first match (~0.2us regardless
of size): a constant-work sample whose 8x growth ratio was pure runner
noise. It flaked at 1.61x per doubling on a loaded CI runner, and,
worse, a genuinely quadratic scan would have PASSED it. The same shape
killed the count-query gate in `3d4cffca`: a hard "Total runtime < 1ms"
EXPLAIN assertion failed at 3.294ms on a loaded runner while the plan
itself was unchanged and correct.

### The fixes, in order of preference

1. **Plan-shape pins instead of wall-clock gates** (best): if the
   invariant is structural, assert the structure. `3d4cffca` pins the
   EXPLAIN plan (Index and Index Only Scan on
   `jobs_actor_pending_idx`, never a Seq Scan) and merely RECORDS the
   execution time for the perf-evidence record. A plan cannot flake.
2. **Self-calibrating control (ratio-of-ratios)**: compare the
   suspect's growth against a known-linear builtin measured in the
   same process, same sizes, same rounds. Runner-wide contamination
   cancels in the outer ratio. Use when no structural pin exists.
3. **min-of-N rounds**: load only ever ADDS time, so the
   least-contaminated round is the closest estimate of the work's own
   cost, and a systematic shape regression raises even the best round.
   The dispatch benchmark (`tests/perf/test_dispatch_benchmark.py`,
   rewritten in `f012a275`) takes the best-round p50 over 5 independent
   rounds against a 50ms gate; p99 is recorded, not gated.
4. **The `load_sensitive` serial lane**: a gate that must compare
   wall-clock samples is load-fragile by nature even with min-of-N; a
   co-tenant that slows only one size's rounds shifts the ratio. Give
   it the serial lane. CI deselects the family everywhere else
   (`-m "not load_sensitive"` in `.github/workflows/ci.yaml`) and runs
   it once, serially; the marker is declared in `pyproject.toml`.

### Executed: the gate and its mutation proof

The probe (shape of `f896d3ec`'s `tests/test_nul_scan_scaling.py` on
its branch; run here against this tree): per input size, min-of-5
rounds of 20 scans, worst-shape payload (every match escaped-literal,
no early return), 1000 versus 8000 matches, gate the 8x-input ratio at
12x:

```
honest:   168.47 us -> 1405.28 us, ratio 8.34x for 8x input  [gate 12x: PASS]
sim   :   100.86 us -> 2894.96 us, ratio 28.70x for 8x input  [gate 12x: FAIL]
```

The `sim` row is the mutation proof: a memcpy-cheap quadratic
simulator (an O(pos) copy per match) injected into the same harness
goes red. The commit's own numbers: linear measures a stable 8.0 to
8.4x; the quadratic sim measured 15.6x there and 16.73x on the red
run. Gate the RAW ratio, not its per-doubling root: the cubic root
compresses a 15.6x quadratic to 2.5x per doubling, under any sane
per-doubling gate. And measure the scan's real worst shape: a payload
the scan can answer early is a constant-work sample that cannot fail
even under a true quadratic.

---

## 6. Races

### Forced-interleave injection (the park-point pattern)

The suite's contention tests do not hope for a scheduler coincidence;
they build the interleaving. The standard instrument is a connection
stand-in that intercepts statements by their SQL shape and parks the
caller at an exact point. Executed file pointers:

- `tests/test_postgres_enqueue_max_pending_lock.py`:
  `_ContendedFakeConn` records every executed statement and models the
  two-tier advisory-lock acquire; `_BlackHoleFakeConn` parks inside
  `execute()` on an `asyncio.Event` and NEVER returns, which is the
  black-hole variant only a client-side budget can bound:

  ```python
  async def execute(self, sql: str, *params: object) -> str:
      if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
          self.blocking_lock_calls += 1
          await self._gate.wait()
          return "OK"
      return await super().execute(sql, *params)
  ```

- The lock-interleaving pins (`c3fe05d9`,
  `tests/test_rt_heartbeat_renewal_cancelwhere_lock_order.py`,
  `tests/test_rt_cancel_sweep2_lock_order.py`,
  `tests/test_batch_abort_flip_bounded.py`) run the same pattern
  against the real backends: hold one operation's row lock at a chosen
  point, run the second operation, assert both complete or one fails
  bounded. The variants matter and are named in the pins:
  **hold-before-execute** (the second op queues behind a lock no one
  has committed yet) and **hold-after-execute-pre-commit** (the first
  op's writes are visible in-transaction while the fence decision
  runs). They are different defects; a harness that can only build one
  of them silently halves the coverage.
- `57b7903b` ("the ghost pins inject the retry deterministically, not
  as a scheduler lottery") applies the same rule to retries: inject
  the retry at the seam, do not arrange conditions and wait to see if
  the retry happens to occur.

Injection at the connection seam beats a real tiny `statement_timeout`
for the same reason `f012a275` documents: a real 1ms budget is itself
a race. When the server's cancel lands after the claim statement
committed, the round half-succeeds and the scenario's precondition
dissolves. The `_FailingDispatcherConn` / `_FailingDispatcherPool`
stand-ins in `tests/test_dispatch_pg.py` make every statement fail
deterministically instead.

### Why "must win at least one row of N trials" is a lottery

An assertion of the form "run the racy scenario N times, the good
outcome must occur at least once" asserts a probability. With a
per-trial win rate that is merely LOW (a scheduling window of
microseconds), N on a fast CI box can easily be zero. Green proves the
window existed sometimes; red proves it didn't this time. Neither
proves the property.

### The deterministic companion test: the no-monopoly pin

The house pattern replaces the lottery with a pin that the racy
outcome is BOUNDED, using full-batch preconditions to make the
interleaving certain. Executed example:
`tests/test_rt_dispatch_fairness_attack.py::TestCrossActorStarvation`
(actor A floods 500 jobs enqueued strictly before B's one job; repeated
limit-10 dispatch rounds run sequentially):

```python
assert first_batch_size == _BATCH_LIMIT, (
    f"first batch dispatched {first_batch_size}, not a full {_BATCH_LIMIT} - "
    "the test only means something when the batch is full yet still carries B"
)
...
assert found_at is not None, (
    f"lone job starved across 5 batches of {_BATCH_LIMIT} behind "
    f"{_FLOOD_COUNT} flooded jobs - fairness sampling lost to FIFO-by-id"
)
assert found_at < _STARVATION_BOUND_BATCHES, (...)
```

The first assert is the pin's honesty clause: a test about fairness
only means something when the batch is full and STILL carries the lone
actor's job. If the arrangement cannot produce a full batch, the test
fails on its own precondition instead of passing vacuously. "B's job
must appear within 2 batches" is deterministic by rank arithmetic
(B holds `pending_rank = 1`), not by probability; nothing waits on a
scheduler.

---

## 7. The leak class

### The shape

The recurring defect: a fixture or test spawns a full bootstrap stack
(worker, leader TaskGroup, notify listener, soak worker) and some exit
path skips cancel-and-await. Because the suite's loops are
module-scoped, the survivors do not die at teardown: they stay parked
on the loop and ADVANCE at every later test's await points. A leaked
task can write process-global state (obs gauge caches, registries) for
the rest of the module.

Teardown must cover every exit path, including the ones that skip the
happy path: `pytest.fail` unwinding, an assertion error mid-act, a
cancellation racing the body. The suite's own doctrine is the
`finally`-based cancel-and-await (see `_stop_loop` in
`test_leader_sweeps_coverage.py` and the soak's
`tests/test_rt_lost_job_soak.py`):

```python
finally:
    if not worker_task.done():
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, BaseException):
            await asyncio.wait_for(worker_task, timeout=60.0)
```

Note the `wait_for` INSIDE the suppression: a cancel that itself hangs
is bounded, so the teardown cannot become the next leak.

### The leftover-task guard

`_fail_on_leaked_asyncio_tasks` in `tests/conftest.py` (autouse,
introduced in `aaa7d9a2`) is the class-level net:

- **What it checks**: a baseline snapshot of `asyncio.all_tasks(loop)`
  before the test body; after the body, any task in `after - before`
  that is still not done fails the LEAKING test by name, with the
  coroutine attached:

  ```
  - task 'soak-worker-0' still pending; coroutine: <coroutine object Event.wait at 0x...>
  - task 'Task-2' still pending; coroutine: <coroutine object Event.wait at 0x...>
  ```

  (executed against the real classifier, `_leaked_pending_task_report`).
- **Baseline diff, not an absolute check**: tasks already pending when
  the test started (a module-scoped fixture's long-lived worker) are
  inheritance, not leak. This keeps the guard quiet in legitimate
  setups but see the blind spots.
- **It excludes its own machinery**: the fixture is itself an async
  generator, so the finally block discards pytest-asyncio's
  per-fixture `async_finalizer` driver task from `after`.

Blind spots, known and accepted:

1. **Sync tests are a vacuous pass** (no loop to leak onto in that
   thread), so a sync test that hands a task to another loop escapes
   the guard.
2. **xdist workers are separate processes**: the guard sees one
   module's loop per process, which is the unit that matters, but
   nothing reports cross-process residue (that class is covered by the
   gauge-cache reset fixtures instead, `aaa7d9a2`).
3. **Inherited tasks stay exempt by design**: if a module-scoped
   fixture leaks, the FIRST consumer test's baseline already contains
   the zombie, and the failure lands nowhere until the module ends.
   The dump of section 2 is the tool for that case.
4. **A task that finishes dirty but quickly** (cancels without awaiting
   the await of a cleanup write) passes the guard and still corrupts
   state; the guard catches liveness leaks, not semantic leaks.

To extend it: the classifier is a module-level function precisely so
`tests/test_suite_hygiene.py` can pin its behavior directly. New
exclusions belong in `_leaked_pending_task_report`'s filtering (with a
comment naming the machinery), never in per-test suppressions. If a
fixture legitimately leaves a long-lived task, register the name in
one place and assert the count is EXACTLY the fixture's own, so a
second leak of the same shape still fails.

---

## 8. Tool inventory

All commands executed while writing this page; output shapes are real.

### Failed lanes, fast

```
$ gh run view --job <job_id> --repo AZX-PBC-OSS/TaskQ --log-failed
```

Job ids come from `gh run view <run_id> --json jobs`. The flag prints
only the failing steps' logs, which turns a 20 minute CI log into the
dozen lines that matter (executed against the coverage job of a real
red run: the header, the plugin line, then the failure).

### The runs-by-SHA matrix

To classify a failure as pre-existing versus regression, run the same
query against the commit SHAs on either side of the boundary:

```
$ gh run list --repo AZX-PBC-OSS/TaskQ --commit <full_sha> --limit 10
completed	failure	...	CI	main	push	35656155816	21m11s	2026-09-21T21:16:04Z
completed	success	...	Deploy Docs	main	push	35656155787	1m27s	2026-09-21T21:16:04Z
completed	failure	...	Release Please	main	push	35656156054	4m49s	2026-09-21T21:16:04Z
```

Red on the parent SHA and red on the child is pre-existing; green on
the parent and red on the child is the child's regression. Use the
FULL sha; the short form matches nothing (executed and confirmed).

### Pre-merge verification

`scripts/verify_merge_ready.sh <PR_NUMBER>` (working copy lives at the
repo root's scripts directory; it refuses to merge on: non-green or
in-flight check runs at the PR's CURRENT head, a stale green from an
older head, missing approval, unresolved conflicts, a branch missing
origin/main, or unpushed local work):

```
$ bash scripts/verify_merge_ready.sh
/home/rich/src/TaskQ/scripts/verify_merge_ready.sh: line 13: 1: usage: verify_merge_ready.sh <PR_NUMBER>
```

### Fault-injection connection patterns (file pointers)

- `tests/test_postgres_enqueue_max_pending_lock.py`: `_ContendedFakeConn`
  (statement-shape interception, GUC save/restore recording),
  `_BlackHoleFakeConn` (unbounded park inside execute).
- `tests/test_dispatch_pg.py`: `_FailingDispatcherConn` /
  `_FailingDispatcherPool` (every statement fails, deterministically);
  `statement_timeout_dispatcher_pool` (the real server-side budget).
- `tests/test_backend_bounded_checkout.py`: `_DeadServerPool` (a
  release that parks against a silently dead server).
- `tests/test_watchdog_safety.py`: intercepted `os._exit` retrieval
  (watchdog trips without killing the test process).
- The SIGALRM-chaining hang-dump plugin of section 3: keep it as a
  scratch conftest in a throwaway run directory, not tracked.
