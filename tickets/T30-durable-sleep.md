# T30 — The durable sleep / wait-until: a general timer beyond the hold

**Status:** LANDED — CLOSED (was: this lane's DRAFT, ranked 1st; the timers lane landed the surface) · **Landed as:** `ctx.wait_until(when, key=…)` — the clock is the hold's RESOLVER: the reserved `__timer__` prefix (invisible to `HitlClient.list`, a human resolve refused), the row is the truth (the held representation, the worker's slot released), the relative-float and absolute-datetime deadlines on the DB clock (never re-based), the due-drain fires at-or-after (the wake's receipt: the key, the scheduled instant, `fired_by='timer'`), the due-drain DRAINS until empty per tick (the partial index on the prefix — the 10K-timer tail cured), and the hold identity carries the MAP INDEX (a timer inside a map item mints EACH sibling its own row). Pins: `tests/test_wf_timer_cures_pins.py`, `tests/test_wf_timer_pins.py`, `tests/test_wf_timer_rv5_pins.py` — green ×2 at the merged estate. The guide's §Pattern 5 teaches the landed face.

## The named pattern

The DURABLE TIMER: the agent's "pause here — no human involved — and resume
at a future instant or when a condition turns true", with the worker gone for
the duration. The frameworks' faces:

- **Temporal**: `workflow.sleep(days)` — the timer is an event in history; a
  worker crash mid-sleep costs nothing (docs.temporal.io/ai, the durable-agent
  framing; the Human-in-the-loop recipe, last updated 2026-08-14).
- **Inngest**: `step.sleep()` / `step.waitForEvent("...", "30 days")` — pause
  for minutes or months, no infrastructure held (inngest.com docs + the
  durable-execution blog, 2026).
- **Trigger.dev**: `wait.for()` / `wait.createToken({timeout: "1d"})` — the
  waitpoint, untriggered, is a row (trigger.dev v4 GA + the waitpoint docs).

## The gap in TaskQflow's landed surface

The landed durable timer is the HOLD's deadline: `ctx.wait_signal((M,),
timeout_s=...)` arms the DB-clock `expires_at` on a signal row, the worker
leader's expiry sweep abandons it, the wait site returns `Expired`. The shape
is right but the CONTRACT is wrong for the timer-only case: an agent that
wants "re-check the upstream ticket tomorrow, no human, no payload" must
declare a payload model it does not want, mint a hold row nobody will
resolve, and wait for a sweep to abandon it — the hold's machinery (the
payload contract, the redact pipeline, the audit, the resolve CAS) is all
dead weight for a plain timer. There is no `ctx.sleep(seconds)`, no
`ctx.wait_until(predicate_or_instant)`, no way to express "the timer IS the
whole wait" without abusing the human-gate shape.

The adoption cost is day-one: an agent-builder arriving from Temporal or
Inngest reaches for the sleep in the first hour (the deep-research loop's
"poll again in 6h", the follow-up agent's "email me if nothing moved by
Friday"). Hitting the hold-shaped workaround reads as "this engine's timer is
a hack", and the honest answer today is: it is.

## The design-first shape

- `ctx.sleep(seconds: float)` and `ctx.wait_until(instant)` — the timer is a
  ROW (the estate's law: rows are the truth), keyed on the node's attempt +
  a timer key; the SAME expiry sweep family arms it (no sleeping worker — the
  slot returns exactly as the hold's does).
- `ctx.sleep_until_condition(fn, *, every_s=..., timeout_s=...)` (LATER row):
  the poll-shaped wait — the engine re-runs a pure check at a cadence, the
  deadline arms the fail-close. v1 ships ONLY the instant timers; the
  condition form needs its own probe (the predicate's side effects are the
  dragon).
- The wait site's union law extends: an expired plain sleep returns
  `Expired` too (the fail-close arm the checker forces — no second timeout
  vocabulary).
- The loop-shape law (E13, one wait per iteration) applies UNCHANGED — a
  timer is a wait for the cursor's purposes.

## The verification bar

- The pin: a sleep-armed node releases its claim (zero occupancy while
  waiting — the no-sleeping-worker law holds for timers, pinned).
- The pin: the timer's expiry rides the DB clock and the sweep family — no
  process-local `asyncio.sleep` in the wait path (a SIGKILL mid-timer
  resumes at the deadline, never early).
- The fence in `docs/guides/agent-loops.md` §Pattern 5 rewrites to the real
  timer (the hold-abuse fence is deleted).

## The receipts

Temporal AI docs (the timer-in-history framing); Inngest durable agent docs
(`step.sleep`, the months-long waits); Trigger.dev v4 waitpoints; the lane's
mapping table row 5 in `docs/guides/agent-loops.md`.
