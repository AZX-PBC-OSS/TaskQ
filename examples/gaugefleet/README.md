# The gauge-fleet recurring refresh

A contrived, self-contained recurring-refresh flow built **only** from the
documented TaskQ mechanisms — the shapes an operator guide would teach for
"refresh something periodically, forever, honestly."

The story: a fleet of gauges reports readings; a refresh cycle turns the new
readings since the last cycle into a digest. Each cycle must be exact (no
reprocessing, no gaps), a slow cycle must never stack on the running one, a
threshold breach must pause for a human, and an unapproved breach must fail
close — loudly, on a clock.

## The four faces (and the doc behind each)

### 1. The durable cursor — `guides/ops.md` §5, Pattern A

`refresh_tick` reads `gauge_cursor` (one row, this example's own table — "The
cursor lives in the payload ... Your cursor is your data"), processes the
increment `updated_at > cursor`, and advances the cursor **in the same
transaction** as the digest upserts. A crash mid-cycle rolls back to an
unmoved cursor; the next cycle re-enters the increment cheaply and idempotently.

### 2. The durable timer — `guides/ops.md` §7, "Scheduled re-enqueue"

Each tick schedules its own successor: `enqueue(refresh_tick, ...,
scheduled_at=now + interval, idempotency_key=f"gaugefleet-tick:{ns}:{n+1}")` —
"The job queue is your timer, and dedup makes the pattern crash-safe." The
successor exists in `scheduled` status **before** this tick does any work, so
a worker death mid-tick cannot kill the chain: the clock holds the door, and
the successor wakes the fleet. A fresh key dimension per successor (the docs'
rule for self-continuation) keeps dedup from eating the chain.

> Why not `@cron`? The cron face is the alternative periodic trigger
> (`examples/actors/ticker.py`, `guides/cron.md`); the chained form is shown
> here because the successor's payload is the natural place to carry the
> cycle's own state, and because a chain makes the timer face (a held
> `scheduled` job an operator can watch) visible on the tick itself. The
> overlap discipline below is the docs' "app-level running-run guard" option —
> a `singleton=True` tick cannot self-schedule its successor (the running tick
> IS an active job of the actor, so the successor's enqueue deterministically
> collides with itself).

### 3. The overlap guard — `guides/ops.md` §5, dedup chooser

`gauge_cycle_guard` is the app-level running-run guard: one row, claimed by an
atomic `UPDATE ... WHERE status = 'idle' OR claimed_at < now() - stale`. A tick
landing while a cycle runs **parks** — it writes a `parked` receipt, schedules
the next tick, and returns. Skip, not raise (a retry storm helps nobody); the
stale-claim window means a dead worker's leftover claim cannot wedge the chain
(the crash-sweep shape). The escalation actor demonstrates the library-level
face too: it is `singleton=True`, and a double-trigger is refused with
`SingletonCollisionError`.

### 4. The HITL pause — `guides/ops.md` §6 + `guides/retries.md`

A reading past the threshold writes a breach row with an approval window and
enqueues `approval_escalate` at `scheduled_at = window close`. When it fires:

- **approved in the window** → the arm returns a typed success;
- **the window lapsed unapproved** → the EXPIRED arm writes a durable
  escalation record FIRST (the operator's audit trail must survive the raise),
  then raises `ApprovalWindowExpiredError` — listed in
  `non_retryable_exceptions`, so the job terminal-fails at attempt one. The
  fail-close is a typed failure an operator can alert on, not a silent skip.

### Along the way

- **Progress** — the tick pushes per-phase progress via `ctx.progress(...)`
  (`guides/progress.md`); the durable `jobs.progress_state` snapshot and the
  live `JobHandle.progress_stream()` both carry it.
- **Fan-out** — a wide increment (>12 gauges) fans out one
  `refresh_gauge` child per gauge via `ctx.jobs.enqueue_batch`, each child
  carrying its own data and a stable per-item idempotency key; the cursor
  advance stays crash-safe both ways (before the enqueue nothing moved; after
  it the children are durable and a re-enqueue dedups).
- **Receipts** — every cycle returns a typed `TickResult` and upserts a
  receipt row (`gauge_cycle_log`): outcome, cursor before/after, counts.

## Run it

Against any Postgres (the drills assume one at
`postgresql://postgres:taskq@localhost:5800/taskq` by default; pass `--dsn`):

```bash
uv run python -m examples.gaugefleet.run_demo --drill all
```

The harness applies TaskQ's migrations, (re)creates the example's tables,
starts a real `taskq worker` subprocess per drill, and runs four drills whose
receipts land in `.measurements/probe-dash/`:

| Drill | What it proves |
|---|---|
| `multicycle` | five consecutive ticks on the self-scheduling chain: per-cycle typed receipts, exact digest/cursor state after each, the wide-increment fan-out arm, the per-phase progress pushes, the terminal admin view |
| `overlap` | a 25 s cycle holds the guard: the chain's own successor AND a manual trigger park, an idle-time trigger is admitted, the singleton double-trigger is refused |
| `cursor` | SIGKILL mid-increment: the uncommitted attempt vanishes (cursor unmoved, digest empty, no receipt row), the chain recovers on the timer, the cursor never rewinds, nothing is digested twice |
| `timer` | the approval window is a `scheduled_at` job: HELD in `scheduled` across a worker death, fires no earlier than scheduled, and — unapproved — runs the EXPIRED arm (durable escalation record + typed fail-close at attempt one); the control arm approves in time |

Individual drills: `--drill multicycle|overlap|cursor|timer`.

## Files

| File | What it is |
|---|---|
| `actors.py` | the three actors, the payload/result models, the typed error |
| `state.py` | the example's own tables and the cursor/guard/breach helpers |
| `run_demo.py` | the drill harness (worker management, receipts, captures) |
