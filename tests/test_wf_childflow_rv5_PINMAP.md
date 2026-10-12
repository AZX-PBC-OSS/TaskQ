# PINMAP — the rv5-child pin pack (the T28 child-flows hostile review)

File: `tests/test_wf_childflow_rv5_pins.py` (drop-in at the repo's
`tests/`; sha256
`0219db45a5e948db785c2dedeed15b14ee0f5efa8797e90aaad00b84c76a9bea`;
`ruff check` + `ruff format --check` clean; pyright: this venv reds the
whole tree pre-existing — the pin file carries ONLY the two error
classes the lane's landed `tests/test_wf_childflow_pins.py` carries
under the identical invocation, no new class).
Tree pinned at `f8bfc5033` (feat/taskqflow-child-flows — the review
head).

Fixtures: the pack's OWN stack, per the lane's prefix law — a
module-minted `chp_rv5_<token>` schema on `TASKQ_TEST_PG_DSN`
(`postgres://taskq:taskq@localhost:5891/taskq`), truncated + reseeded
per test, dropped CASCADE at teardown; module-local `wf_conn` /
`wf_schema` / `wf_sql` / `wf_pool` shadow the conftest-registered
`tests/_wf_fixtures.py` fixtures so the ALWAYS-ON G7 rows↔status
teardown (auto-registered on every `test_wf_*` async test) runs against
the pack's schema. All 7 pins are `pytest.mark.integration`. The drive
helpers (`_spawn_and_hold`, `_node_row`) re-spell the lane's own
`tests/test_wf_childflow_pins.py` idioms (a pin file is self-contained
— packs never import each other). `wf_pool`'s teardown TERMINATES
(`pool.terminate()`), never graceful-closes: the F-C4 red's
RecursionError destroys an asyncpg holder-release task mid-flight and
`close()` waits on it forever (the seam is documented in the fixture's
docstring + RECEIPTS.md).

Convention for the "red verbatim" column: the first two lines of the
pin's OWN failure signature in the unmarked run (`red-run-raw.txt`) —
the asserting line and the first `E`-line(s) carrying the conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-C1 (SEVERE, the gather dragon) | `test_rv5_c1_concurrent_spawns_in_one_body_each_mint_their_own_child` | `assert result.distinct_children == 3, (`<br>`E   AssertionError: F-C1: 3 concurrent spawns aliased 1 child flow id(s) — the inputs of the dropped spawns were silently discarded (the gather dragon)`<br>`E   assert 1 == 3` (where the GatherOut shows `verdicts=['found:d1-0']×3`) | xfail-strict (integration) |
| F-C2 (SEVERE, the retention-respawn) | `test_rv5_c2_a_missing_child_root_at_the_wake_is_a_loud_fail_close` | `assert result.verdict == "failed", (`<br>`E   AssertionError: F-C2: the purged child's join resolved 'ok' — the wait's ChildFailed fail-close arm is the LOUD answer to a missing child root (never a silent respawn-and-join)` | xfail-strict (integration) |
| F-C3 (SEVERE, the recursion bomb) | `test_rv5_c3_a_self_spawning_flow_is_refused_loudly_and_bounded` | `assert census == 1, (`<br>`E   AssertionError: F-C3: a self-spawning flow minted 14 runs in 12 ticks — unbounded flow growth from ONE create_flow (the recursion bomb; no cycle check, no depth bound)` | xfail-strict (integration) |
| F-C4 (the cascade's stack) | `test_rv5_c4_the_cancel_cascade_is_iterative_over_a_deep_chain` | `assert cancelled is not None, (`<br>`E   AssertionError: F-C4: the cancel cascade blew the Python stack at depth 1200 (RecursionError) — 248 of 1200 runs left NON-terminal, the call raising instead of reporting a bounded partial failure` | xfail-strict (integration) |
| F-C5 (the orphan window) | `test_rv5_c5_the_cascade_walks_both_directions_no_child_escapes` | `assert child_root["status"] == "cancelled", (`<br>`E   AssertionError: F-C5: the unreceipted child escaped the parent's cancel (child root 'running') — the cascade walked the receipts only and never the parent_flow_id provenance it could see; the orphan runs on with its parent dead` | xfail-strict (integration) |
| F-C6 (the dev-tick expiry gap) | `test_rv5_c6_the_dev_drive_s_tick_runs_the_child_join_expiry_arm` | `assert outcome == "terminal", (`<br>`E   AssertionError: F-C6: the dev drive never delivered the join's Expired arm — the parent held past its own timeout_s=0.2 deadline for the whole drive ('max_ticks'); sweep_expired_signals is wired in the fleet leader sweeps only, never in FlowRunner.tick` | xfail-strict (integration) |
| F-C7 (the no-op resolve bounce) | `test_rv5_c7_resolving_a_child_join_hold_is_a_named_boundary_refusal` | `assert result.status == "refused", (`<br>`E   AssertionError: F-C7: resolving a child-join hold returned 'delivered' — the delivery resolves NOTHING (the wait site re-holds on the still-live child; the operator's decision is a dead letter). The boundary must refuse the machine-owned hold, naming its kind` | xfail-strict (integration) |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv5-c1** — K concurrent `ctx.spawn_flow` in one body (the natural
  `asyncio.gather` spelling) mint K DISTINCT child runs, one per spawn;
  each child's work sees its OWN input; the fan-in returns each child's
  own verdict in order (`['found:d1-0', 'found:d1-1', 'found:d1-2']`);
  the parent row carries K distinct receipts.
* **rv5-c2** — the child the parent joined being GONE at the wake (the
  retention purge's shape) is a LOUD fail-close: the wait returns the
  `ChildFailed` member carrying a NAMED, non-empty error class; the
  parent terminalizes on its own ChildFailed arm; and the child census
  stays 0 — NO new child run is minted (never a silent respawn, never
  double execution).
* **rv5-c3** — a self-spawning flow is REFUSED: the spawn raises, the
  run terminal-FAILS with a non-empty error class naming the refusal,
  and the workflow's run census stays at EXACTLY the ONE run the caller
  created (the 12-tick attack drive mints nothing).
* **rv5-c4** — the cancel cascade over a depth-1200 receipt chain
  completes ITERATIVELY: returns 1200, zero roots left non-terminal,
  and the call never raises the Python stack (no RecursionError, no
  half-cancelled tree).
* **rv5-c5** — the cascade walks BOTH directions: a child whose create
  committed (the `parent_flow_id` provenance riding the create's own
  tx) but whose receipt was never written (the kill window) is
  CANCELLED with its parent — root and node rows terminal-cancelled.
* **rv5-c6** — the dev driver's `tick` runs the expiry arm: a child
  that OUTLIVES its join deadline across ticks yields the `Expired`
  member to the resumed parent; the drive terminalizes with the body's
  `expired` verdict (never `max_ticks` with the parent held past its
  own deadline).
* **rv5-c7** — resolving a child-join hold is REFUSED at the boundary
  with the hold's KIND named (`child…` in the reason); the hold
  SURVIVES `held`; the node keeps its held representation (nothing
  consumed, nothing bounced).

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-C1's cure shape** — the pin asserts the DISTINCT-CHILDREN shape
   (K spawns → K runs → K verdicts), the finding's first option. The
   alternative it names — the engine REFUSES concurrent spawns loudly,
   the refusal naming the sequential spelling — is NOT pinned: if
   atomic slot-claiming is rejected and the refusal is the cure,
   rewrite the pin to: the drive terminal-fails the node with the
   refusal's named error class, the child census stays ≤ 1, and the
   error message names the sequential spelling. The conviction (the
   cursor read at `_childflow.py:414-423` and the write at `:457-467`
   are separate autocommit statements; the wake-resume's fresh attempt
   re-reads slot 0 — the gather aliases forever) stands either way.
2. **F-C2's error class name** — the pin requires the ChildFailed arm +
   a NON-EMPTY error class + census unchanged; the cure picks the
   class's name (e.g. `ChildFlowMissing`). If the cure instead teaches
   the spawn's arbiter a DURABLE memory (the receipt row consulted
   before any re-create — the spawn sees the receipt, reads the missing
   root, and the wait's `_childflow.py:224` fail-close finally
   reaches), the pin stands unchanged. If the cure goes the other way
   (retention never purges a joined child), the pin's staging becomes
   impossible BY CONSTRUCTION — rewrite it as the retention-fence pin.
3. **F-C3's seat and scope** — the pin convicts the DIRECT self-spawn
   (the live-convicted shape) at the RUNTIME seat: the spawn is
   body-interior, opaque to the build, so "build-time where provable"
   does not reach this shape today; the runtime loud refusal (the
   receipt walk makes the ancestor check cheap) is what the pin
   asserts: census bound + terminal-failed + named error. The A→B→A
   ancestor case is the same walk's generality — not separately pinned
   (the finding's live conviction is the self-spawn); the cure's walk
   must cover it. If a build-time static proof of spawn literals lands,
   the pin's staging (which never reaches `app.get` refusal because the
   body is opaque) still convicts the runtime path — keep both.
4. **F-C4's alternative** — the pin asserts the ITERATIVE shape. The
   finding's documented alternative (depth-bounded with a loud
   partial-failure report NAMING THE SURVIVORS) requires a rewrite:
   the call returns/raises a typed partial failure carrying the
   survivors' ids, and the pin asserts the report's contents. Note the
   survivor count is host-stack-dependent (248 under pytest, 216
   standalone, 983↔984 cancelled — the class is the conviction).
5. **F-C5's staging fidelity** — the orphan window is staged
   DETERMINISTICALLY through the spawn's OWN create path
   (`create_flow(run_key="childflow:…:0", root_metadata_extra={parent_*})`
   — the byte-identical rows `_childflow.py:429-437` writes), minus the
   receipt write (`:457-467`) the kill aborts. The pin is the
   cascade-walk LAW (both directions), not a timing drill — no trigger,
   no SIGKILL, no race. If the cure instead makes the receipt atomic
   with the create (one tx — the window closed by construction), the
   pin's staged state becomes unrepresentable; rewrite the pin as the
   atomicity pin (a receipt-less child with provenance is refused at
   create). The pins' assertions (provenance present, receipt absent,
   child cancelled with the parent) are the walk's contract either way.
6. **F-C6's honest staging (READ THIS)** — the pin stages a child that
   OUTLIVES the deadline ACROSS TICKS (parked on a never-resolved
   signal). The finding's observed face — "a slow child returns ChildOk
   DESPITE timeout_s=1.0" — is the COMPLETING-child face: in the dev
   drive the child's in-process body blocks ONE tick, and under the
   deadline's documented AT-LEAST semantics (the sweep fires at the
   first pass at-or-after `expires_at`, never at a precise wall time)
   the row-is-the-truth read in `wait()` legitimately wins that race —
   ChildOk after an in-tick completion is arguably CORRECT. The
   conviction the pin carries is the arm's ABSENCE: with the child
   parked across ticks, the deadline passes and NOTHING abandons the
   hold — the drive runs to `max_ticks` with the parent held past its
   own deadline; the Expired face is dead code on the dev path. The
   preferred cure (per the finding): the expiry arm lands in
   `FlowRunner.tick` beside `sweep_child_joins`. The alternative — the
   dev path REFUSES `timeout_s=` loudly — satisfies the finding IF
   NAMED and requires a pin rewrite (the wait raises the named refusal
   at hold-registration under the dev driver).
7. **F-C7's boundary** — pinned at `HitlClient.resolve` (the seat every
   surface rides: the CLI's `flows resolve` verb calls it after its own
   gate validation). The refusal must name the hold's kind (the pin
   accepts any reason containing `child` — the registered context kind
   is `child_join`, the signal prefix `__child__:`). The
   audit-on-refusal row the resolve already writes is unchanged. The
   flip drill patched exactly this seat (+10 lines in
   `_boundary_verdict`) and the pin went XPASS-strict — the mechanics
   and the seat both proven.

## Unreproducible findings

NONE — all 7 findings re-convicted live at `f8bfc5033` in the scratch
copy before pinning (the pre-pin probe outputs are quoted verbatim in
RECEIPTS.md). One environment wrinkle, documented for provenance: the
q8b attack script hardcodes its schema (`ch_atk8b`, ignoring
`CH_SCHEMA`) and its first re-run here wedged at its own
`pool.close()` after the RecursionError (the same asyncpg seam the
pack's `wf_pool` fixture defuses with `terminate()`) — the conviction
itself re-landed cleanly (984/1200, RecursionError after 8.2s) on the
re-driven probe.
