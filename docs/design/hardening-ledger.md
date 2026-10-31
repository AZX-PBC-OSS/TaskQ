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

## Ledgered follow-ups (the reviewed cures' known open faces)

A reviewed cure's known-but-open face, itemized with the same rigor as the
fix holes above: what the curing commit covers, what stays open, and the
discipline the open part's future cure owes.

- **The chains' priority DECLARATION face** (the wf-priority cure's
  ledgered follow-up — the feature review's own itemization). The
  priority surface's precedence law resolves every row it materializes —
  except a chain link's own declaration, which has no face today:
  `EmitChild`, `chain_start`, and `Chain.next_child` (chain.py's emit
  child specs, the chain's fork children) accept no `priority` field, so
  `chain_fork` always resolves the column default — a consumer declaring
  per-chain-link priorities silently lands every link at 0, the exact
  silent-drop shape the priority cure's other faces refuse at their
  declaration sites. COVERED by the wf-priority cure: the rows the OUTBOX
  DRAIN materializes (the loop escalation's consumer — an interactive
  run's most latency-sensitive follow-up — and the fired joins' fresh
  item-shape consumers) resolve the run's stamped `wf_priority` (the
  drain's root-metadata join + COALESCE, the fleet door's parse-and-
  degrade arithmetic mirrored in SQL; pinned red-first in
  tests/test_wf_priority_pins.py: the escalation lands at the run's
  override, a malformed or out-of-domain stamp degrades to the column
  default, never wedges the drain). OPEN: the chains' DECLARATION face —
  a `priority=` on the chain's links (the `EmitChild` field, the
  `chain_start`/`next_child` plumbing, and the smallint-domain refusal at
  the declaration site — `check_priority_domain`, the same law every
  other priority face keeps). The future cure's red-first pin: a chain
  declaring per-link priorities lands its emit-start and link rows at
  the declared values (today's rows are the default-0 conviction).
- **The drain's regex guard passes beyond-int4 digit strings to the
  `::int` cast** (the wf-priority cure's drain residual — the
  re-review's own ledger line). `OUTBOX_FETCH_UNDELIVERED_SQL`'s stamp
  resolution guards the stamp text with `^[+-]?[0-9]+$` and then casts
  `(metadata->>'wf_priority')::int` inside the smallint `BETWEEN` guard
  (_sql_sweep.py:326-328). A digit string past int4's domain
  ("99999999999" — the regex PASSES it) makes the CAST RAISE in
  Postgres (`value ... is out of range for type integer`), erroring the
  whole drain statement — where the fleet door's Python parse degrades
  the same stamp to no-override (Python's int is arbitrary-precision;
  `check_priority_domain` is the clamp). The two doors diverge on
  exactly the stamp a hand-crafted row can carry. CORRUPTION-ONLY and
  unreachable via the API: `create_flow`/`run` validate the kwarg
  (`check_priority_domain`) before the stamp is ever written — only a
  hand-crafted row renders the shape, and it is the review's own
  prescribed regex-guard shape's property, not an introduced defect's.
  Blast radius if hit: one bad root parks EVERY flow's consumer drain
  behind it (the fetch is a shared batch — the raise kills the pass).
  OPEN (the future hardening's shape, either arm): length-bound the
  regex so the cast cannot see a digit string past int4 (a
  `{1,9}`-shaped digit cap), or cast `::numeric` FIRST and put the
  smallint `BETWEEN` on the numeric — the raise becomes the guard's own
  false-arm, the degrade reaching the COALESCE untouched. The
  red-first pin's design: seed a flow root with
  `wf_priority="99999999999"` and one undelivered outbox row, run
  `drain_outbox` — today the statement raises (the drain wedges; the
  red), post-hardening the consumer lands at the column default 0 and
  the outbox flips delivered (the green — the same degrade-to-default
  law the in-domain malformed stamps already pin).

## Follow-ups (deferred, with the judgment stated)

- **The ratelimit elapsed-vs-ceiling pins** (test_ratelimit_pg_row_lock_bounded,
  test_ratelimit_sliding_window_pg_lock; ~15 pins). Each asserts a
  single-shot, server-bounded wait against a ceiling with 12-20x deliberate
  headroom (a 100 ms budget under a 2.0 s ceiling), and no CI run has red
  one receipt-class (a runner-starvation red of a timing pin). Marking the
  family would move ~15 pins into the serial load-sensitive lane - a
  lane-capacity decision deferred until the estate earns it. The stated
  judgment: mark on the first receipt-class red, not on class resemblance
  alone.

- **The system-e2e window family's marks** (13 files / 29 pins,
  unmarked): test_compound_failures 3, test_kill9_pg_crash 1,
  test_kill9_redis_broker 2, test_ladder_rolling_deploy 1,
  test_leader_loss 1, test_operational_loop 1, test_partition_recovery
  1, test_rolling_release_fleet 7, test_rolling_deploy 1,
  test_shutdown_requeue_states 2, test_wf_deploy_matrix 6,
  test_wf_operator_march 2, test_redis_outage 1 — all the convicted
  marches' class (scenario verdicts riding monotonic settle windows).
  The routing must land TOGETHER with these marks plus a RE-PRICED
  lane cap from a MEASURED serial wall (the round-2 bound-derived band
  ~65-130 min of added serial wall -> a cap in the ~180 territory,
  priced from measurement, not from the bounds) — never flip one half
  alone (the staged-flip doctrine: runner AND parallelism move as one,
  ci.yaml's own law). These 13 files carry NO per-test timeout
  markers, so their marks are only sound with --timeout=900 in the
  load-sensitive lane's command (which the routing already carries).

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
