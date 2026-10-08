# FINDING (phase-3 engine, reported for the fixer): F-P4-HELD-DRIVE-NULL-DEADLINE

`FlowRunner.drive(until="held")` NEVER returns "held" for a NO-DEADLINE
hold — it spins `max_ticks` (5000 ticks, ~126 s of full-tick polling).

## The mechanism
- `wait_signal(..., timeout_s=None)` is the LEGAL form (the W1 warning's
  subject: "a workflow that waits forever on a human" — explicit, allowed).
- `register_hold` writes the held representation with `scheduled_at =
  NULL` (the deadline expr for None).
- `_HELD_COUNT_SQL_TEMPLATE` (`api/_runner.py:376`) reads:
      WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'pending'
        AND scheduled_at > now()
  `scheduled_at > now()` is NULL for a NULL deadline → not counted →
  `_any_held` is FALSE forever → the driver polls 5000 ticks.

## The probe (executed, PG 18 container on :5697)
- flow: one step, body `await ctx.wait_signal((Approval,), reason=...)`,
  NO timeout, no gates declared.
- `drive(fid, until="held")` → returned "max_ticks" after 126.82 s
  (probe: tq_cli_probe2, 2026-10-07).
- WITH `timeout_s=120.0` → "held" in < 0.5 s (all 15 phase-3 hitl pins
  carry timeout_s=120 — they dodge the bug by construction).
- The rows are CORRECT the whole time (signals row 'held', the node's
  metadata.hold set) — the driver's QUESTION is wrong, not the hold.

## The precise fix (one line, the fixer's call)
The hold marker is the rows-alone truth:
      AND metadata ? 'hold'
replaces `AND scheduled_at > now()` (the claimable query already
excludes held rows via the same marker — one representation, two
spellings today).

## Boundedness (the livelock clause)
The driver's max_ticks cap BOUNDS the spin (it returns, it does not
hang) — but 5000 full dispatch polls for a legal input is a defect: the
same class the addendum's "nothing stuck" clause names. Red pin:
tests/attack4_wf_held_drive_livelock.py (strict-xfail until the fixer
lands the cure).
