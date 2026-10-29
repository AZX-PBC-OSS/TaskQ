# THE ENGINE-FACES PINS — the red-first receipts

The observability front's three reported ENGINE findings, cured red-first
(the engine lane: `_sql_status.py`, `worker/_leader_sweeps.py`'s
vocabulary, `workflows/api/_validate.py` — K7 owns `_runner.py`'s
finalize/ladder face; disjoint). The front's pins + faces arrived via the
port of feat/verify-obs @ 248fac447 (the merge at the base of this
branch); the ENGINE findings behind them are fenced here.

## CURE 1 — E5's list-element hole (the typed wiring's whole promise)

**The finding:** the drill wired a `list[Word]` producer into a
`list[str]` consumer — **BUILD-TIME VALIDATION PASSED** and the run died
at RUNTIME with a pydantic ValidationError on every attempt. The typed
wiring's law (the checkers + `validate()` catch the mismatch BEFORE the
run) had an element-type blind spot: the LIST level was checked (the
bodyless-join arm, `gather`'s element preservation), the ELEMENT level
was not.

**Mechanism (the reads decided it):** the validate-time probe —
`_rule_consumer_compat`'s LIST-PRODUCER arm (the new `_element_compat`
comparison): a body producing `list[PE]` wired into a consumer's
`list[CE]` param is the ELEMENT-level compatibility (both models → the
issubclass door; a model element into a resolved non-model element → the
drill's conviction; a list producer into a single-model param → the
certain decode death; the duck-shaped/unresolvable/RootModel doors → the
zero-false-positive skips). The WIRE-TYPE WALK was probed first and
refused honestly: a precise `step` overload for the single-promise list
seam cannot convict while a permissive fallback overload exists — the
fallback always matched last (the gate red receipt below is that probe's
evidence). The checkers' element face is the HANDLE flow — the covariant
promise + the invariant list — drill-typed in the corpus.

**RED receipts (at the pre-cure head 248fac447, the merge):**

- `tests/test_wf_validate_pins.py::test_incompatible_list_element_mutation`
  — RED: `validate()` passed the drill (`rules == set()`, the expected
  `E5-incompatible-consumer` missing). Capture:
  `.measurements/runs/engfaces-red-pins.txt`.
- THE TYPE GATE — RED: the wiring-site probe's `MUST_ERROR` marker
  unflagged on BOTH pinned checkers (pyright 1.1.414 AND ty 0.0.85) —
  the wire-type walk's impossibility receipt. Capture:
  `.measurements/runs/engfaces-red-type-gate.txt`.

**THE TYPE-PROBE PACK:** `tests/typeprobe/wf_list_element_negative_types.py`
(the corpus wired into `_gate.py`) — the drill's element mismatch pinned
on the handle flow (`Promise[list[Word]]` is not `Promise[list[str]]`,
both checkers), the matching-element green face, and the wiring-site
decoded-payload line as the documented clean line (validate()'s face).
**The marker count's growth: 39 → 40** (the gate's own count).

## CURE 2 — the reclaim-requires-a-leader silence

**The finding:** a lapsed lease with a DEAD FLEET waits forever, silently
— the rows say "running" to the end of time.

**Mechanism:** the STATUS DERIVATION'S INPUT (the sweep-stamp face was
rejected by the reads: the reclaim sweep runs only under a leader — with
the fleet down nothing ever stamps the row, so a stamp would lie by
absence). `WORKFLOW_NODES_SQL` selects the lease columns at the source;
the running face derives the verdict from the row's own
`lock_expires_at`: **"RECLAIMABLE, awaiting a leader"** — the holder is
presumed dead, the reclaim sweep repends within `TASKQ_SWEEP_INTERVAL`
(default 30 s) once a leader is up; with no leader the row waits. The
live-lease rows keep the ordinary RUNNING face (the negative faces pin
the absence).

**RED receipts (at 248fac447):** the marker pins RED —
`TypeError: FlowNodeRow.__init__() got an unexpected keyword argument
'lock_expires_at'` (no surface carried the lease columns; the marker did
not exist). Capture: `.measurements/runs/engfaces-red-pins.txt`.

## CURE 3 — workflow_nodes_sql's missing columns

**The finding:** `_sql_status.py`'s query didn't select
`started_at`/`failed_parent` — the CLI bought them with its own
supplement read (two reads where one suffices).

**Mechanism:** the columns selected AT THE SOURCE (`WORKFLOW_NODES_SQL`:
`j.started_at`, `j.metadata->>'failed_parent'`, plus the lease face's
`j.lock_expires_at`/`j.last_heartbeat_at`); the CLI's supplement read
RETIRED — `_flows_status` issues ONE jobs fetch and the supplement's
faces (the running age, the dead parent NAMED) keep working through the
engine query.

**RED receipts (at 248fac447):** the one-read pin RED (the counting conn
saw TWO jobs fetches — the node query + the supplement) and the
source-law pin RED (the query named none of the columns). Capture:
`.measurements/runs/engfaces-red-pins.txt`.
