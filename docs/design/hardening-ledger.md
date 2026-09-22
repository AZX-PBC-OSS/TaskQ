# The hardening ledger: flake families, fix holes, and the classes they belong to

Every defect class this campaign closed, with the mechanism that produced it
and the discipline that keeps it closed. Companion to the debugging playbook:
the playbook teaches the method; this records what the method found. Cite
issue numbers rather than PR state - issues outlive boards.

## Flake families (root-caused and killed)

Each family: the symptom CI showed, the mechanism, the kill, and the pin.

1. **The scheduling lottery** (test_rt_cancel_sweep2_lock_order: `swept >= 1`
   across trials). A race test asserted a scheduling outcome - the sweep wins
   a row - which a starved runner legally inverts. The contract is
   conservation (every row terminal exactly once, counts reconcile); the
   property behind the lottery (a bulk cancel cannot monopolise the backlog
   because each batch commits and releases its locks) is pinned
   deterministically: a content-gated witness parks the drain mid-batch, the
   sweep wins rows by construction.

2. **Cache-resident timing gates** (test_nul_scan_scaling: 16.50x for 8x on
   CI, 8.0-8.4x locally). A raw growth-ratio gate carries the runner's cache
   bias: the small payload is cache-resident, the large one is not. Killed by
   self-calibration: an algorithmic twin (the scan's own loop shape minus the
   per-match work) measured in the same probe run; the gate reads the excess
   of ratios. Known boundary, stated in the pin: the gate sees
   interpreter-level superlinearity; a C-level O(pos) per-match term measures
   under the gate and must be caught in review.

3. **The command-budget starvation family** (heartbeat chaos, extend
   reservation leases, renewal probes). Timing-sensitive choreography rode
   connections carrying the 2s command_timeout; under parallel-CI load the
   budget expires and the assertion never under test reds. Kill: dedicated
   no-timeout connections for test choreography (assertions untouched), and
   for the renewal probe, production semantics: a failed beat is tolerated
   exactly as production tolerates it (the isolate line at 3 consecutive).

4. **The teardown leak family** (the soak's worker bootstrap surviving into
   later tests). Teardown gave the graceful stop 60s while the worker's own
   exit bound is 85s+8s: the give-up fired mid-cleanup, the second
   cancellation interrupted cleanup, and residue (an armed os._exit watchdog,
   executor handles) degraded the next test 14x and force-exited a pytest
   process a module later. Kill: two-stage teardown bounded above the
   worker's own bound, cancel+await+report of every minted task on every exit
   path, and the leftover-task guard snapshotting at call end (its blind spot
   was timing, not scope).

5. **The bucket-span lottery** (rt-diff timestamp buckets). The differential
   harness's now-bucket ignored the scenario's own wall span; a runner slow
   enough to cross a bucket boundary inverted the expectation. Kill: the
   bucket covers the scenario's wall span (PR 394).

6. **The clock-domain straddle** (cron singleton parity). Seeds read the
   runner clock while the tick's due predicate reads the PG clock; a ~12ms
   skew across a grid boundary reds the strict-future assert. Kill: seeds
   anchored to the PG clock domain; the skew-injection proof (red pre-fix
   with a 5ms-behind fake clock, green post) is the standard for any
   two-clock test.

## Fix holes (found by red-team review of the fixes themselves)

A fix gets attacked by a separate agent. These are the holes the fixes
shipped with; each was closed on the fixing branch before merge.

- **Uncommitted deliverable**: a fix whose template edits existed only in the
  worktree - the committed tree failed its own suite. The red-team's
  first check is now: does the committed tree contain everything the report
  claims.
- **Vacuous pins**: a warning-existence pin that stayed green with the
  warning deleted; an injection that never proved it injected into the
  tracked jobs. Both replaced with pins that fail under a named mutation.
- **Guard windows**: the terminal-batch refusal covered the bulk arms but not
  the single member-write paths (a TOCTOU on both backends); the commit
  gate's hook suppression keyed a throwaway per-acquire proxy (25 stacked
  listeners in 25 acquires); the isolate exclusion window existed but was
  inert - proven inert, then pinned, rather than assumed.
- **Wrong predicates**: the fanout warning reused the rate-limit gate's
  url-or-provider check and mis-warned in both directions (a false positive
  on the managed-identity shape, a false negative on a dead fanout); the
  rotation claim was fiction for live streams (the serializer bakes the
  secret at construction). Predicates must test the exact object the
  behavior keys on.
- **Silent degradation**: an audit trail whose backend-mediated half degraded
  to a warning with no metric - a compliance trail that could vanish forever,
  unalertable. Degradation paths get a counter and an alert story, or the
  mutation fails closed.
- **The alert-less counter**: the prune tier's new error counter had no
  consumer on the alert plane - the drain still read healthy everywhere an
  operator looks. A metric without a consumer is decoration.
- **Unbounded waits**: a hung session verifier froze the SSE generator inside
  its own fail-closed path. Every injected check gets a bound and a logged
  timeout, or fail-closed means fail-hung.
- **Absorbed crashes**: teardown reaping that suppressed a residue task's
  real exception. Silent swallowing is a defect even in test infrastructure.

## The rules the campaign operates under

- A rerun is not evidence. Every red gets the chain: symptom, mechanism,
  root cause, deterministic repro, fix, red/green, mutation sharpness.
- Tests assert behavior through public surfaces. Scheduling order, wall
  clocks, and runner properties are not contracts; conservation, exactly
  once, and honest accounting are.
- Load lotteries are defects. Deterministic interleave or fault injection
  replaces every must-win-in-N-trials assertion.
- The twin must mirror production or map its divergence in one place.
  Documented drift is still drift.
- Every fix is attacked by a separate agent before it merges.
