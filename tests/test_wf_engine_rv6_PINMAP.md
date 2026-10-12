# PINMAP — the rv6-engine pin pack (the rv6 round's engine convictions)

File: `tests/test_wf_engine_rv6_pins.py` (drop-in at the head's
`tests/`; sha256
`4ef99791683ebaa79712a7b3fdb6931337f230bcd3cb41155537bbd6a4bf9e35`;
ruff-clean and pyright-clean against the repo configs — see RECEIPTS.md's
Environment leg for the venv/PYTHONPATH seam).
Tree pinned at `0e2f5cc096727adc5d373d1f7dd813d10b10d331` (the pushed
head).
House fixtures used: `wf_conn` / `wf_schema` / `wf_pool` /
`module_pg_schema` (tests/_wf_fixtures.py via tests/conftest.py);
`TASKQ_TEST_PG_DSN` for the PG lane. Both pins are marked `integration`.
The staging is SQL-direct (the finding's own "stage the born-on-cancelled
child via SQL" / the held-siblings staging): the F-E1 child row is the
drain's documented exemption window's shape (`pending`, `deps_pending=0`,
no hold, on the cancelled root); the F-E2 siblings ride the PRE-cure hold
identity (`(workflow_id, node_key, signal_name, hold_epoch)` — distinct
signal names per sibling, the only shape the unique index admits).

Convention for the "red verbatim" column: the pin's OWN failure
signature's first lines in the unmarked run (`red-run-raw.txt`) — the
assertion line and the first `E`-line carrying the conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-E1 (SEVERE; the in-process claim path carries no flow-status leg — a bare `tick()` executes a body on a CANCELLED flow) | `test_e1_the_in_process_claim_never_executes_on_a_terminal_flow` | `assert _E1_SENTINEL == [], (`<br>`E   AssertionError: F-E1: the body EXECUTED on the cancelled flow — the in-process claim carries no terminal-flow fence (the fleet's dispatch SQL has it). Sentinel: ['e1-body-ran']` | xfail-strict (integration) |
| F-E2 (the hold→node wake matches step_key + flow + hold-marker — no map_index leg, no ORDER BY; a middle-out resolve wakes the wrong sibling) | `test_e2_a_middle_out_resolve_wakes_the_holds_own_node` | `assert resumed == {own_node}, (`<br>`E   AssertionError: F-E2: the middle-out resolve woke the WRONG sibling — … The resolved hold 01a12cf3-bfb… owns node 01a12cf3-bfb… (map_index=1); the resumed set is ['01a12cf3-bfb'] — …` | xfail-strict (integration) |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv6-e1** — on a flow whose root is CANCELLED, a bare
  `FlowRunner.tick()` (the driver's public dispatch pass, never gated by
  `drive()`'s loop-top check) executes NOTHING: the staged child's body
  never runs (the sentinel stays empty) AND the row never finalizes
  executed-succeeded (status ≠ `succeeded`, `attempt` unspent). The cure
  may sit in the claimable SELECT (`CLAIMABLE_NODES_SQL_TEMPLATE`) or the
  claim CAS (`NODE_CLAIM_SQL_TEMPLATE`) — the fleet's dispatch fence
  (`backend/_dispatch_sql.py`'s `_WF_DISPATCH_FENCE_TEMPLATE`, "never on
  a TERMINAL flow" — with the escalation-kind exemption this pin's
  one-step flow never exercises) carries both seats' semantics; the pin
  reads the OUTCOME and tolerates either seat.
* **rv6-e2** — resolving the MIDDLE of three held map siblings resumes
  EXACTLY the hold's own node (the row whose `metadata->>'hold'` IS the
  resolved hold's id — the stamp `register_hold` already writes) and
  leaves the two unanswered siblings' held representations intact. This
  is the map_index-addressed resume's semantic (the 1528719d2 cure's own
  words: "one hold, one node, one resume; a resolve can never wake a
  sibling") pinned against THIS tree's pre-cure shape.

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-E1's cure seat** — the pin reads the outcome (body never ran, row
   never executed-succeeded), so the SELECT-seat cure AND the CAS-seat
   cure both pass. If the CAS alone is cured, `tick()` returns `ran=True`
   (the row was enumerated, the claim refused) — the pin allows this
   deliberately (the fence's own doctrine: "a refused claim is
   re-derivable; a claimed one is not un-runnable").
2. **F-E1's escalation exemption** — the fleet's fence exempts the loop's
   registered escalation step (born into a terminal flow BY DESIGN, the
   page-a-human duty). The pin stages a PLAIN step, so the exemption
   never enters; a cure must keep the exemption narrow (the pin would
   convict a blanket "terminal flows execute children" rule).
3. **F-E2's pin seat** — pinned at the deliver path's wake (the
   finding's named leg, `deliver_payload`). The expiry sweep carries the
   SAME wake-by-step_key shape (`sweep_expired_signals`' node lookup);
   curing `deliver_payload` alone greens this pin while the sweep keeps
   the hole — flagged as a follow-up pin candidate (same assertion
   shape, the expiry arm's door).
4. **F-E2's staging vs the pre-cure identity** — the siblings hold
   DISTINCT signal names because this tree's hold-identity unique index
   `(workflow_id, node_key, signal_name, hold_epoch)` forbids same-name
   concurrent holds across siblings (the 1528719d2 notes: the same-name
   shape UniqueViolates the INSERT here — the worker-mode face of the
   same finding). The pin convicts the WAKE leg; the identity leg's
   broadening (map_index in the index) is the cure commit's other half
   and is NOT required to flip this pin.
5. **F-E2's determinism** — the wrong-sibling pick rides heap order
   (insertion order; no ORDER BY in the wake). The pin's assertion (the
   resumed set must be EXACTLY the hold's own node) convicts ANY wrong
   pick, so a platform whose scan order differs still reds — the
   determinism note documents the observed order, the assertion does not
   depend on it.

## Unreproducible findings

NONE — both findings reproduced live at
`0e2f5cc096727adc5d373d1f7dd813d10b10d331` before pinning (the pre-pin
repro script + output are in `repro/` and RECEIPTS.md; the F-E1 leg was
ALSO reproduced by the front at base 37ab05b8 per the finding's note).
No probe artifacts of my own needed correction. The finding's commit
pointer for the hold/map identity cure read "37147ad26"; the cure is in
fact `1528719d2` (37147ad26 is its fixtures-retune follow-up) — NEITHER
is an ancestor of this tree, which is what the pin convicts.
