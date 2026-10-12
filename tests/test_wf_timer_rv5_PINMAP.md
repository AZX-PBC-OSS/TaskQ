# PINMAP — the rv5-timer pin pack (the T28 durable-timer hostile review)

File: `tests/test_wf_timer_rv5_pins.py` (drop-in at the branch's
`tests/`; sha256
`5693b0bdf37994d70edefa13e43678d09ca062c80fab064e61ca0a0833496fea`;
ruff-clean and pyright-clean against the repo configs — see
RECEIPTS.md's Environment leg for the venv/PYTHONPATH seam).
Tree pinned at `08dccb92d` (feat/taskqflow-durable-timers), based on
`37ab05b8`.
House fixtures used: `wf_conn` / `wf_schema` / `wf_pool` /
`module_pg_schema` (tests/_wf_fixtures.py via tests/conftest.py);
`TASKQ_TEST_PG_DSN` for the PG lane. The five PG pins are marked
`integration`; the two static pins (the migration index, the template
render) carry no marker. The redeploy staging (F-T5, F-T6) rides the
versioning pins' own `_install_registry` seam (the registry map IS the
deploy) plus their `_claimable`/`_run_node` mid-flow stop. The async PG
pins pick up the estate's always-on G7 rows↔status teardown
automatically (file prefix `test_wf_`): the states the red paths leave
(a held run pre-cancel, a `running` root over the forever-pending
'second') reconstruct `running`/`held` — agreement, no false teeth;
both runs carried zero teardown errors.

Convention for the "red verbatim" column: the pin's OWN failure
signature's first lines in the unmarked run (`red-run-raw.txt`) — the
assertion line and the first `E`-line carrying the conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-T1 (SEVERE; the trojan `__timer__` model mints an invisible/unanswerable/never-fail-closing hold that self-fires into SignalPayloadError) | `test_t1_wait_signal_refuses_a_timer_prefixed_model` | `assert node["error_class"] != "SignalPayloadError", (`<br>`E   AssertionError: F-T1: the run died the TROJAN death — the minted '__timer__Approval' hold was invisible to the client and unanswerable by hand, then the TIMER ARM fired it at the deadline and the wait site's revalidation killed the run (SignalPayloadError). The mint itself must be refused` | xfail-strict (integration) |
| F-T2 (timers leak onto the human-hold broadcast + backfill) | `test_t2_the_broadcast_and_backfill_never_surface_timer_holds` | `assert timer_creations == [], (`<br>`E   AssertionError: F-T2 (notify leg): the listener announced a TIMER hold as a HUMAN hold — HoldCreated events for the clock's own rows: [('__timer__leak', 'notify')]` | xfail-strict (integration) |
| F-T3a (10k due timers drain ~50 min at the 30s cadence; no drain) | `test_t3a_the_timer_arm_drains_the_due_set_in_one_cascade` | `assert remaining == 0, (`<br>`E   AssertionError: F-T3a: one sweep cascade left 1400 of 1500 due timers HELD (the arm fires one bounded batch per pass — at the 30s default cadence a 10k backlog takes ~50 minutes to drain)` | xfail-strict (integration) |
| F-T3b (no index leads the due-scan) | `test_t3b_a_due_scan_index_exists_in_the_migrations` | `assert due_scan, (`<br>`E   AssertionError: F-T3b: no wf_signals index covers expires_at — … The shipped indexes: ['CREATE UNIQUE INDEX wf_signals_hold_identity_uniq', 'CREATE INDEX wf_signals_id_status_idx', 'CREATE INDEX wf_signals_run_status_idx']` | xfail-strict |
| F-T4 (the admin run page renders Resolve on a timer row) | `test_t4_timer_rows_never_render_the_human_resolve_form` | `assert "rv5t4timer" not in forms, (`<br>`E   AssertionError: F-T4: the TIMER hold row renders the human resolve form — a control whose every submission is refused-as-forgery is a false affordance on the operator's page` | xfail-strict |
| F-T5 (the mixed-version run is flagged nowhere) | `test_t5_flows_status_surfaces_the_version_drift` | `assert drift_lines, (`<br>`E   AssertionError: F-T5: \`taskq flows status\` never mentions the version drift — the run carries 2 distinct code_versions and the operator sees nothing. …` | xfail-strict (integration) |
| F-T6 (the renamed node zombies the in-flight run forever) | `test_t6_a_vanished_node_key_terminal_fails_loudly` | `assert node["status"] == "failed", (`<br>`E   AssertionError: F-T6: the vanished node's run NEVER terminalizes — 'second' is 'pending' and the root follows it (the eternal zombie …). The loud law: terminal-fail with a NAMED error class` | xfail-strict (integration) |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv5-t1** — the trojan mint is refused with the reserved-prefix rule
  NAMED: EITHER the build/validate door raises (the message carries
  `__timer__` + "reserved"), OR the wait site dies LOUDLY on the FIRST
  attempt — the node terminal-`failed` with an error class that is NOT
  `SignalPayloadError` and a message naming the reserved-prefix rule.
  The convicted lifecycle (minted → invisible to `list()` →
  unanswerable → fail-close-proof → self-fired into the payload
  mismatch) is unreachable under either cure.
* **rv5-t2** — across a timer's whole lifecycle the listener emits NO
  `HoldCreated` for a `__timer__` signal (live notify AND backfill
  legs), and no `Backfilled.open_hold_ids` carries the timer id. A
  TYPED `TimerScheduled`-style event distinct from `HoldCreated` also
  satisfies the pin (it convicts the human-hold shape only).
* **rv5-t3a** — one sweep call cascade (`sweep_due_timers(...,
  drain=True)` — the natural face; the head's `TypeError` falls back to
  the plain call and reds) leaves ZERO of the 1500 seeded due timers
  held, all 1500 `delivered`, in <15s (a handful of seconds in
  practice; the bound is CI headroom, not the semantics).
  Baseline-relative counters make the pin order-independent within the
  module's shared schema.
* **rv5-t3b** — the migrations carry a `wf_signals` index covering
  `expires_at` (the `(status, expires_at)` partial, the plain
  `expires_at`, or the equivalent the cure writes). The pin reads the
  `CREATE INDEX` statements out of `src/taskq/migrations/*.sql` (the
  estate's static-read pattern) and keeps a witness assertion that SOME
  `wf_signals` index exists (so a rename reds loud, never vacuous).
* **rv5-t4** — rendering the run page (the admin factory's own jinja2
  Environment construction, autoescape on) with one timer hold + one
  human hold, both `held` with actions enabled: the HUMAN row keeps its
  `data-wf-form="resolve"` (the real affordance is not amputated) and
  the TIMER row renders none.
* **rv5-t5** — `taskq flows status <run>` (the house's CliRunner +
  `to_thread` + env-settings idiom) on a run whose node rows carry 2
  distinct `code_version` stamps (the pin stages the drift itself via
  the redeploy seam and asserts the staging landed) carries a drift
  line: "version" + "drift"/"differ"/"mixed" on one output line.
* **rv5-t6** — the run whose pending node's key vanished from the
  registry terminal-FAILS: the vanished node's row is `failed` with a
  non-empty `error_class` and a message naming the key/the registry/the
  redeploy cause, and the root row is `failed`. The raw
  `WorkflowRunError` escape (today's shape) is caught by the pin's
  `suppress(Exception)` so the pin reads the ROWS — the eternal zombie
  is the conviction, not the escape's text.

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-T1's refusal seat** — the pin accepts the build door OR the wait
   site. NOTE for the wait-site seat: the refusal raised INSIDE the body
   rides the ladder — the pin's `max_attempts=1` makes the first
   refusal terminal; a cure that wants the deterministic route (the
   ladder unburned) should register the refusal error in
   `is_deterministic_authoring_failure`. The pin's assertions hold under
   both (they read the terminal row, not the ladder's counters).
2. **F-T2's event alternative** — the finding's "typed TimerScheduled
   event" direction is NOT pinned against: the pin convicts only the
   HoldCreated/Backfilled human-hold shape, so the filter cure AND the
   typed-event cure both pass. If the maintainers want the typed event
   ASSERTED (not merely allowed), add a leg awaiting it.
3. **F-T3a's face** — the pin calls `drain=True` (the finding's named
   face) with the TypeError fallback so the knob's absence is the red's
   CONTENT rather than its crash. If the cure instead makes drain the
   DEFAULT semantics (no knob — every pass loops), the pin greens
   unchanged (the fallback call drains). The 15s bound is wall-clock
   headroom for a contended CI box; the probe fired 100 in 0.02s, so
   1500 is sub-second at this hardware class.
4. **F-T3b's equivalent-shape tolerance** — the pin keys on
   `expires_at` appearing in a `wf_signals` index definition. A cure
   indexing an expression (e.g. a partial index whose predicate carries
   the prefix filter) still contains the column name; a cure that
   moves the due-scan OFF wf_signals (a dedicated timer table) needs
   the pin re-pointed at the new table — flagged.
5. **F-T4's level** — pinned at the template render (the finding's
   named seat). If the cure lands in the VIEW instead (timer rows never
   enter the holds projection, or arrive with a kind flag the template
   already honors), rewrite the pin's staging to drive the view's
   projection; the assertion (no resolve form for the timer row, the
   human row's kept) stands.
6. **F-T5's surface** — pinned on `flows status` (the finding's named
   face). The admin run page carrying the same drift banner satisfies
   the law but not this pin; flag the twin surface as a follow-up pin
   (the stamps' read is one SQL).
7. **F-T6's direction** — pinned as the LOUD terminal failure (the
   house's loud law), per the finding's pick. The documented
   alternative — a sweep arm that NAMES the stranded runs + the
   cancel-then-restart remediation surfaced on the run view — does NOT
   satisfy this pin (a named-but-living zombie is still a zombie to the
   run's own status); if the maintainers rule the remediation direction,
   rewrite the pin to assert the sweep's naming + the surfaced
   remediation. The dev-driver raw escape today (WorkflowRunError out
   of `drive()`) is ALSO not the cure — the fleet never runs the dev
   driver, and an operator's `flows status` still reads `running`.

## Unreproducible findings

NONE — all seven findings reproduced live at `08dccb92d` before pinning
(the pre-pin repro script + output are in `repro/` and RECEIPTS.md).
One probe artifact of MY OWN was corrected before pinning (the
convictions stand): the F-T5 probe leg invoked the CLI inside the
probe's own event loop (the CLI's `asyncio.run` refused; the pin runs
it through `to_thread`, the house's idiom — the red convicts cleanly).
