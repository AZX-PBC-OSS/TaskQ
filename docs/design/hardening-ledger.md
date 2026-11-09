# The hardening ledger: defect classes and the discipline that keeps them closed

The defect classes the debugging method has closed, each with the
mechanism that produced it and the discipline that keeps it closed.
Companion to the debugging playbook: the playbook teaches the method;
this catalogues what the method finds. Cite issue numbers rather than
PR state - issues outlive boards.

## Flake families (root causes and their kills)

Each family: the symptom, the mechanism, and the discipline that kills it.

1. **The scheduling lottery.** A race test asserts a scheduling outcome -
   the sweep wins a row - which a starved runner legally inverts. The
   contract is conservation (every row terminal exactly once, counts
   reconcile); the property behind the lottery (a bulk cancel cannot
   monopolise the backlog because each batch commits and releases its
   locks) is pinned deterministically: a content-gated witness parks the
   drain mid-batch, the sweep wins rows by construction.

2. **Cache-resident timing gates.** A raw growth-ratio gate carries the
   runner's cache bias: the small payload is cache-resident, the large
   one is not. The kill is self-calibration: an algorithmic twin (the
   scan's own loop shape minus the per-match work) measured in the same
   probe run; the gate reads the excess of ratios. Known boundary,
   stated in the pin: the gate sees interpreter-level superlinearity; a
   C-level O(pos) per-match term measures under the gate and must be
   caught in review.

3. **The command-budget starvation family.** Timing-sensitive
   choreography riding connections that carry the 2s `command_timeout`
   expires the budget under parallel-CI load and the assertion under
   test never fires. Choreography runs on dedicated no-timeout
   connections (assertions untouched); where the test mirrors production
   semantics, a failed beat is tolerated exactly as production tolerates
   it (the isolate line at 3 consecutive).

4. **The teardown leak family.** Teardown gives the graceful stop less
   than the worker's own exit bound: the give-up fires mid-cleanup, the
   second cancellation interrupts cleanup, and residue (an armed
   `os._exit` watchdog, executor handles) degrades the next test and
   force-exits a pytest process a module later. The kill is two-stage
   teardown bounded above the worker's own bound, cancel+await+report of
   every minted task on every exit path, and a leftover-task guard
   snapshotting at call end.

5. **The bucket-span lottery.** A differential harness's now-bucket that
   ignores the scenario's own wall span inverts the expectation whenever
   a slow runner crosses a bucket boundary. The bucket covers the
   scenario's wall span.

6. **The clock-domain straddle.** Seeds reading the runner clock while
   the tick's due predicate reads the PG clock fail strict-future
   asserts on a ~12ms skew across a grid boundary. Seeds anchor to the
   PG clock domain; the skew-injection proof (red with a 5ms-behind fake
   clock, green after the fix) is the standard for any two-clock test.

## Fix-review checks

A fix gets reviewed on its own merits after it works. These are the
holes fixes ship with when nobody looks:

- **The committed tree fails its own suite** - template edits that exist
  only in the worktree. First check: does the committed tree contain
  everything the report claims.
- **Vacuous pins**: a warning-existence pin that stays green with the
  warning deleted; an injection that never proves it injected into the
  tracked jobs. A pin must fail under a named mutation.
- **Guard windows**: a refusal that covers the bulk arms but not the
  single member-write paths; a gate keyed to a throwaway per-acquire
  proxy; an exclusion window that exists but is inert. Prove the window,
  then pin it - never assume it.
- **Wrong predicates**: a warning that reuses another gate's
  url-or-provider check and mis-warns in both directions; a rotation
  claim that is fiction for live streams because the serializer bakes
  the secret at construction. A predicate tests the exact object the
  behavior keys on.
- **Silent degradation**: an audit trail whose backend-mediated half
  degrades to a warning with no metric. Degradation paths get a counter
  and an alert story, or the mutation fails closed.
- **The alert-less counter**: a metric without a consumer on the alert
  plane is decoration - the drain still reads healthy everywhere an
  operator looks.
- **Unbounded waits**: a hung session verifier that freezes an SSE
  generator inside its own fail-closed path. Every injected check gets a
  bound and a logged timeout, or fail-closed means fail-hung.
- **Absorbed crashes**: teardown reaping that suppresses a residue
  task's real exception. Silent swallowing is a defect even in test
  infrastructure.

## Open faces (known, itemized, unfixed)

- **The chains' priority DECLARATION face.** The priority surface's
  precedence law resolves every row it materializes - except a chain
  link's own declaration, which has no face today:
  `EmitChild`, `chain_start`, and `Chain.next_child` accept no
  `priority` field, so `chain_fork` always resolves the column default -
  a consumer declaring per-chain-link priorities silently lands every
  link at 0, the silent-drop shape the priority surface's other faces
  refuse at their declaration sites. COVERED by the priority surface:
  the rows the OUTBOX DRAIN materializes resolve the run's stamped
  `wf_priority` (the drain's root-metadata join + COALESCE, the fleet
  door's parse-and-degrade arithmetic mirrored in SQL; pinned in
  `tests/test_wf_priority_pins.py`, which fails on the unfixed engine):
  the escalation lands at the run's override, a malformed or
  out-of-domain stamp degrades to the column default, never wedges the
  drain. OPEN: a `priority=` on the chain's links (the `EmitChild`
  field, the `chain_start`/`next_child` plumbing, and the
  smallint-domain refusal at the declaration site - `check_priority_domain`,
  the same law every other priority face keeps). The future fix's pin:
  a chain declaring per-link priorities lands its emit-start and link
  rows at the declared values (today's rows are the default-0 shape).
- **The drain's regex guard passes beyond-int4 digit strings to the
  `::int` cast.** `OUTBOX_FETCH_UNDELIVERED_SQL`'s stamp resolution
  guards the stamp text with `^[+-]?[0-9]+$` and then casts
  `(metadata->>'wf_priority')::int` inside the smallint `BETWEEN` guard
  (`src/taskq/workflows/_sql_sweep.py`). A digit string past int4's
  domain ("99999999999" - the regex PASSES it) makes the CAST RAISE in
  Postgres (`value ... is out of range for type integer`), erroring the
  whole drain statement - where the fleet door's Python parse degrades
  the same stamp to no-override (Python's int is arbitrary-precision;
  `check_priority_domain` is the clamp). The two doors diverge on
  exactly the stamp a hand-crafted row can carry. CORRUPTION-ONLY and
  unreachable via the API: `create_flow`/`run` validate the kwarg
  (`check_priority_domain`) before the stamp is ever written - only a
  hand-crafted row renders the shape. Blast radius if hit: one bad root
  parks EVERY flow's consumer drain behind it (the fetch is a shared
  batch - the raise kills the pass). OPEN (either arm): length-bound
  the regex so the cast cannot see a digit string past int4 (a
  `{1,9}`-shaped digit cap), or cast `::numeric` FIRST and put the
  smallint `BETWEEN` on the numeric - the raise becomes the guard's own
  false-arm, the degrade reaching the COALESCE untouched. The pin's
  design: seed a flow root with `wf_priority="99999999999"` and one
  undelivered outbox row, run `drain_outbox` - today the statement
  raises (the drain wedges), post-hardening the consumer lands at the
  column default 0 and the outbox flips delivered (the same
  degrade-to-default law the in-domain malformed stamps already pin).

## Deferred, with the judgment stated

- **The ratelimit elapsed-vs-ceiling pins**
  (`test_ratelimit_pg_row_lock_bounded`, `test_ratelimit_sliding_window_pg_lock`;
  ~15 pins). Each asserts a single-shot, server-bounded wait against a
  ceiling with 12-20x deliberate headroom, and no CI run has failed on
  the runner-starvation flake class. Marking the family would move ~15
  pins into the serial load-sensitive lane - a lane-capacity decision
  deferred until the estate earns it. The standing judgment: mark on the
  first observed failure of that class, not on class resemblance alone.

- **The system-e2e window family's marks** (13 files / 29 pins,
  unmarked): test_compound_failures 3, test_kill9_pg_crash 1,
  test_kill9_redis_broker 2, test_ladder_rolling_deploy 1,
  test_leader_loss 1, test_operational_loop 1, test_partition_recovery
  1, test_rolling_release_fleet 7, test_rolling_deploy 1,
  test_shutdown_requeue_states 2, test_wf_deploy_matrix 6,
  test_wf_operator_march 2, test_redis_outage 1 - all of that
  march class (scenario verdicts riding monotonic settle windows).
  The routing must land TOGETHER with these marks plus a re-priced
  lane cap priced from a measured serial wall - never flip one half
  alone (the staged-flip doctrine: runner AND parallelism move as one,
  ci.yaml's own law). These 13 files carry no per-test timeout
  markers, so their marks are only sound with `--timeout=900` in the
  load-sensitive lane's command (which the routing already carries).

## The discipline

- A rerun is not evidence. Every failure gets the chain: symptom,
  mechanism, root cause, deterministic repro, fix, failing/green proof,
  mutation sharpness.
- Tests assert behavior through public surfaces. Scheduling order, wall
  clocks, and runner properties are not contracts; conservation, exactly
  once, and honest accounting are.
- Load lotteries are defects. Deterministic interleave or fault injection
  replaces every must-win-in-N-trials assertion.
- The twin must mirror production or map its divergence in one place.
  Documented drift is still drift.
- Every fix is attacked by a separate agent before it merges.
