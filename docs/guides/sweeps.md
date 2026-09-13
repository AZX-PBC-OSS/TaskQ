# Sweeps

A **sweep** is a recurring pass over a population of rows that need work: candidates to screen,
records to refresh, searches to evaluate, files to extract. It is the most common shape of
background work after plain request-triggered jobs, and it is the shape that most often ships
looking correct and draining at a fraction of the rate its author intended.

This guide prescribes one shape and names the four failure modes that come from departing from
it. The shape is:

> **PAGE** (keyset cursor) → **FAN OUT** (one job per item) → **RECURSE** (self-enqueue with the
> advanced cursor).

Cron's job in that shape is to *start* the chain if it is not already running. Cron is not the
pacing mechanism, and the moment it becomes one, your throughput ceiling is the cron period
rather than your fleet.

This page is the pattern-level companion to
[ops.md §5 — Fan-out at scale](ops.md#5-fan-out-at-scale-chunks-cursors-idempotency), which
covers the three chunking shapes, batch finalizers and the idempotency-key discipline. Read that
section for the enqueue-API tradeoffs; read this one for how to build a sweep that terminates,
scales, and can be proven to have covered its population.

---

## Prerequisites

- A cursor column on the swept population — see [the cursor-column rule](#the-cursor-column-rule)
  for the one property it must have.
- `taskq migrate up` applied, and a worker consuming the queues the root and leaf actors declare.
- Familiarity with `ctx.jobs.enqueue` for sub-job enqueue ([actors.md](actors.md)) and with the
  autonomous-vs-transactional distinction in
  [ops.md §5](ops.md#pattern-a-cursor-chain-recommended-default).

---

## The cron period becomes the throughput

Here is the sweep almost everyone writes first. It selects a fixed page, fans out, notices it did
not reach the end, logs that fact, and returns.

```python
@actor(name="screen_candidates", queue="cron")
async def screen_candidates(payload: EmptyPayload, ctx: JobContext[EmptyPayload]) -> None:
    rows = await fetch_eligible(limit=250)          # a fixed page
    for row in rows:
        await ctx.jobs.enqueue(screen_one, OnePayload(id=row.id))
    if len(rows) == 250:
        logger.info("screen-capped", capped=True)   # and then nothing happens
```

Nothing here raises, no metric turns red, and the log line even tells you the truth. But the
actor returns after one page and the next page waits for the next cron tick. The batch limit
*reads* like a slice size — how much to do at once — and *behaves* like a rate limiter: a hard
ceiling of `page_size` items per cron period, permanently.

The arithmetic is unforgiving, because the ceiling is a product of two numbers chosen for
unrelated reasons:

| Page size | Cron period | Ceiling | Time to drain 10,000 |
|---|---|---|---|
| 250 | hourly | 250/hour | 40 hours |
| 500 | twice daily | 1,000/day | 10 days |
| 50 | hourly | 50/hour | 8 days |

Those are not pathological configurations. Each number is defensible alone: 500 rows is a
reasonable page, and twice a day is a reasonable refresh cadence. The product is a ten-day drain
for work the fleet could finish in an hour or two, and nothing in the system says so.

Two consequences are worth separating, because they fail differently:

**Coverage becomes unprovable.** A sweep that always stops at the page boundary has no idea
whether it ever reached the end of its population. "Green with `capped: true` every pass" and
"green having covered everything" produce identical output.

**Arrival rate can permanently exceed drain rate.** If the population grows by more than
`page_size` per period, the backlog grows without bound and the sweep still reports success on
every pass. There is no self-correction in a fixed-page sweep: falling behind is a stable state,
not a transient one.

!!! danger "`capped: true` in a log is a bug report, not telemetry"
    A sweep that logs its own truncation and returns has diagnosed itself and then done nothing
    about it. If a page can be full, the pass is not finished, and the actor's job is to continue
    — not to record that it stopped. Treat any "capped", "truncated", or "more rows remain" log
    line in a sweep as an unfinished implementation.

---

## The shape that scales

Split the sweep into two actors. A **root** pages the population and enqueues work; a **leaf**
does one item. The root's last act, if its page was full, is to enqueue *itself* with the
advanced cursor.

```python
from datetime import timedelta

from pydantic import BaseModel

from taskq import JobContext, actor

PAGE_SIZE = 500
MAX_LINKS = 1_000  # runaway ceiling, not the terminator — see below


class SweepPayload(BaseModel):
    run_id: str
    cursor: str | None = None  # keyset cursor: last (created_at, id) seen
    link: int = 0              # how many pages deep this chain is


class ItemPayload(BaseModel):
    run_id: str
    item_id: str


@actor(
    name="refresh_sweep",
    queue="cron",
    start_to_close=timedelta(minutes=5),
    # No unique_for, no singleton: a successor must be free to enqueue while
    # its own predecessor is still `running`. Single-flighting belongs on the
    # root's trigger, not on the chain. See "Single-flight the chain" below.
)
async def refresh_sweep(payload: SweepPayload, ctx: JobContext[SweepPayload]) -> None:
    rows = await fetch_page(after=payload.cursor, limit=PAGE_SIZE)

    for row in rows:
        try:
            await ctx.jobs.enqueue(
                refresh_item,
                ItemPayload(run_id=payload.run_id, item_id=row.id),
                # Per-item key: a root retry re-enqueues every item of its page.
                # Autonomous sub-enqueues commit immediately and do not roll back
                # with the parent, so this key is what keeps the retry idempotent.
                idempotency_key=f"refresh:{payload.run_id}:{row.id}",
            )
        except Exception:
            # Per-item isolation: one malformed row must not end the page.
            logger.exception("refresh-enqueue-failed", item_id=row.id)

    if len(rows) < PAGE_SIZE:
        return  # short page: the population is exhausted, the chain ends here

    if payload.link >= MAX_LINKS:
        logger.error("refresh-sweep-link-ceiling", run_id=payload.run_id, link=payload.link)
        raise RuntimeError(f"link ceiling {MAX_LINKS} hit; cursor may not be advancing")

    next_cursor = encode_cursor(rows[-1])
    await ctx.jobs.enqueue(
        refresh_sweep,
        SweepPayload(run_id=payload.run_id, cursor=next_cursor, link=payload.link + 1),
        # The cursor is IN the key, so the successor cannot collide with this
        # job's own succeeded row. See "Idempotency by role" below.
        idempotency_key=f"refresh-page:{payload.run_id}:{next_cursor}",
    )


@actor(name="refresh_item", queue="default", start_to_close=timedelta(minutes=2))
async def refresh_item(payload: ItemPayload) -> None:
    await do_the_work(payload.item_id)
```

And the trigger, which is now a starter rather than a pacer:

```python
from taskq import cron

# Every 15 minutes: start a chain if one is not already in flight. The cadence
# controls how promptly a new population is noticed, NOT how fast it drains.
cron("*/15 * * * *", "refresh_sweep_root")
```

Four properties follow from this shape, and each is the direct negation of a failure mode above:

**Throughput is decoupled from the cadence.** A full page enqueues its successor *immediately*.
The chain runs as fast as the fleet dispatches it, bounded by concurrency and rate limits — the
things you actually sized — rather than by the cron period.

**Termination is data-driven and self-evident.** A short page enqueues nothing, so the chain ends
exactly when the population is exhausted. Coverage is no longer an open question: the chain
either reached a short page or it is still running.

**Failure loses at most one page.** A leaf failure is one item. A root failure re-runs one page,
and the per-item keys make that re-run a no-op for the items already enqueued.

**Progress is durable without any accounting.** The cursor lives in the payload of a committed
job row. There is no in-memory position to lose and no run table to reconcile.

!!! note "`Snooze` is for a job that comes back; self-enqueue is for a chain that advances"
    `raise Snooze(delay)` reschedules *the same job row* — right for polling one condition, and it
    does not consume the retry budget. A sweep wants a *new* job per page so that each page's
    success is recorded independently and a failure cannot lose the pages before it. Use
    self-enqueue for paging and `Snooze` for waiting.

---

## Why keyset, not `OFFSET`

`OFFSET n` asks the database for a position in a result set it must re-derive on every query.
That is correct only if the result set is stable between pages. A sweep's eligible set is the one
thing guaranteed *not* to be stable: the sweep exists to make rows stop being eligible.

Consider an eligible set of ~10,000 rows and `OFFSET` pagination, where each page's items become
ineligible as the leaves process them:

- Rows leave the set **from the middle**, so every subsequent offset slides *backwards* relative
  to the data. A page at `OFFSET 2000` now begins at what was row 1,700.
- Rows that collapse backwards past the cursor are **never visited** — they moved below an offset
  the walk has already passed.
- Rows that remain ahead of the shrinking region are **visited repeatedly**, because the offsets
  keep sliding onto them.

Both errors happen at once, and they do not cancel out. A measured walk of this shape made 14,500
visits over a ~9,900-row population and never visited 27% of it. The sweep looked productive — it
was doing more work than the population had — while a quarter of the rows were untouched.

Keyset pagination has no such failure, because the cursor is a *value in the data*, not a count:

```sql
-- The cursor is the last row seen. Rows vanishing from the middle are
-- irrelevant: the predicate still means "strictly after this value".
SELECT id, created_at
FROM candidates
WHERE eligible
  AND (created_at, id) > ($1, $2)   -- decoded cursor
ORDER BY created_at, id
LIMIT 500;
```

Every row is visited at most once, no row is skipped because of a concurrent deletion, and the
query uses an index range scan rather than counting rows it discards. Order by a tuple ending in a
unique column (`id` above) so the order is total and the cursor is unambiguous.

TaskQ uses exactly this construction for its own read APIs — `client.list(...)` returns a
`JobPage` with an opaque keyset `cursor` ([jobs-clients.md](jobs-clients.md#jobpage)). That cursor
paginates *TaskQ's* tables; your sweep's cursor over *your* data is yours to encode, and it
belongs in the job payload.

### The cursor-column rule

State it as a rule, because it is violated in a way that looks perfectly reasonable:

> **A cursor is monotonic only over a column the walk does not write.**

An LRU-style sweep — "refresh the least recently synced rows first" — invites the mistake. Order
by `last_synced_at` ascending, take the oldest page, refresh it. But the refresh *stamps
`last_synced_at`*. The rows the leaves just processed get a new, larger value, which places them
**ahead** of the cursor rather than behind it. The chain re-serves its own completed work, and
because every pass produces a full page, it never terminates. It is a sweep that appears
maximally busy and has a completion rate of zero.

The fix is to keep the ordering you want and move the cursor to a column the walk cannot touch:

| Want | Order by | Cursor on | Safe? |
|---|---|---|---|
| Oldest-synced first | `last_synced_at` | `last_synced_at` | **No** — the leaves write it |
| Oldest-synced first | `last_synced_at` | `(id)` with a snapshotted eligible set | Yes |
| Oldest-created first | `(created_at, id)` | `(created_at, id)` | Yes — immutable |
| Whole population | `(id)` | `(id)` | Yes — immutable |

The general forms: cursor on an immutable key (`created_at`, `id`, or a per-run snapshot of the
eligible ids), and let the *selection* predicate express freshness instead of the ordering. If
LRU order genuinely matters, capture the eligible id list into a run-scoped table at link zero and
page that snapshot — it is immutable by construction, and it makes coverage auditable.

---

## Idempotency by role

This is the rule that silently kills chains, and it follows from one line of SQL. TaskQ's
idempotent enqueue is:

```sql
ON CONFLICT (idempotency_scope, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
```

There is **no status predicate**. The `WHERE` clause matches the partial unique index
(`jobs_idempotency_scope_key_uniq`) — it is not a filter on job status. A key therefore collides
against a row in *any* status, `succeeded` included, for as long as that row physically exists in
`jobs`. Rows leave only when the prune sweep archives them: 30 days for `succeeded` and
`cancelled`, **90 days for `failed`** and `abandoned`/`crashed` by default.

So a recursive successor carrying the same idempotency key as its predecessor collapses onto that
predecessor's own `succeeded` row. `DO NOTHING` fires, the follow-up SELECT returns the dead
predecessor, the caller gets a handle with `was_existing=True`, and **no job is created**. The
chain stops after one link. No exception, no failed job, nothing in a dashboard — the enqueue
"succeeded" and returned a handle to a job that finished hours ago.

Conversely, the `unique_for` preflight *does* filter on status, but it only runs at all when
**both** `unique_for` and `identity_key` are present:

```python
if args.unique_for is not None and args.identity_key is not None:
```

Miss either half and the dedup is a silent no-op (with a once-per-actor warning on
`JobsClient.enqueue`, and no warning at all from `SubJobEnqueuer.enqueue`). When it does run, it
matches only jobs in `unique_states` — default `("pending", "scheduled", "running")` — inside the
`unique_for` window, so a completed predecessor does not block a new run.

The three roles in a sweep therefore want three different answers, and they are not
interchangeable:

| Role | `idempotency_key` | `unique_for` + `identity_key` | Why |
|---|---|---|---|
| **Root** (cron trigger) | no | **yes** | Status-scoped, live-only: suppresses a second chain while one is in flight, and stops suppressing once it finishes. A key here would be either useless (fresh per tick) or fatal (stable, so the second-ever tick dedups onto tick one forever). |
| **Successor** (self-enqueue) | **yes — containing the cursor** | **no** | The cursor makes each link's key distinct, so a link cannot collide with its own predecessor, and a re-run of one link is still idempotent. `unique_for` here would make the successor dedup against its own still-`running` parent — the chain would end at link one. |
| **Leaf** (per item) | **yes — item id + run id** | no | A root retry re-enqueues the whole page; the key makes that a no-op per already-enqueued item. |

Two traps sit either side of the root row:

**A root without `unique_for` stacks chains.** Every cron tick starts a fresh walk over the same
population while the previous walks are still running. The work is duplicated (the leaves' keys
absorb some of it, but only within a run scope) and the queue depth multiplies by the number of
overlapping chains.

**`unique_for` on a *thin* root does not single-flight the chain.** The root finishes in seconds —
it only pages and enqueues — so the window frees while its descendants are still working. See
[Single-flight the chain](#single-flight-the-chain) for what to do instead.

!!! warning "Never give a successor a key its predecessor could have had"
    The concrete form of this bug: `idempotency_key=f"sweep:{run_id}"` on a self-enqueue. Link one
    creates it; link two collides with link one's `succeeded` row and vanishes. The cursor (or the
    link index) must be *in* the key. This is the same rule as
    [ops.md's "no stable key on a self-continuation successor"](ops.md#choosing-a-dedup-mechanism),
    stated from the chain's side.

### Version components that do not version

A related failure comes from keys built out of a component chosen to "version" the work, where the
component does not move under the conditions that matter.

**A row's `updated_at` does not move when a job is cancelled.** Key a stage on
`f"extract:{file_id}:{row.updated_at}"` and you have bound the key to a timestamp the *handler*
would have advanced. Cancel the job and the handler never ran, so `updated_at` is unchanged, so
every re-enqueue for the whole retention window computes the same key and collides with the
cancelled corpse. Measured shape: three files wedged for 3.5 days across roughly 330 sweep passes
with zero re-enqueues, while every pass reported success. Cancellation is the common trigger
because it is the one terminal status reached *without* the handler executing.

**Ordinal keys collide across parents.** `f"{run_id}:{index}"` looks unique and is not, if
`index` restarts per parent: link 2's chunk 0 has the same key as link 1's chunk 0, which
succeeded. The second chunk 0 silently never runs — work loss, not duplication. Derive keys from
something globally distinguishing (the item's own id, or the cursor plus the ordinal), never from
a position that resets.

The general test for any key component: **can this value stay the same across two enqueues that
must both produce a job?** If yes, it is not a version. Terminal-but-handler-never-ran (cancelled,
deadline-expired) and per-parent ordinals are the two cases that fail it most often.

---

## Per-item error isolation

A sweep enumerates rows it did not choose, which means it will eventually meet one it cannot
parse. If that row's exception escapes the loop, the pass dies for every *other* row too — and
because the pass is a job, it retries into the same malformed row and dies again. One bad record
takes out the hour.

```python
for row in rows:
    try:
        await ctx.jobs.enqueue(refresh_item, build_payload(row), idempotency_key=key_for(row))
    except Exception:
        # Skip-and-continue. `build_payload` is the usual culprit: a
        # ValidationError from one row is not a reason to stop the page.
        logger.exception("sweep-item-failed", run_id=payload.run_id, item_id=row.id)
```

Catch broadly *at the per-item boundary* and nowhere else. The distinction that matters:

- **Per-item faults** (a `ValidationError` on one row, one unparseable field) are skip-and-continue.
  Log with the item id so the row is findable, and keep going.
- **Pass-wide faults** (the database is gone, the cursor cannot be decoded) must propagate. They
  are exactly what the retry policy is for, and swallowing them turns a broken sweep into a
  green one.

A per-item counter is worth emitting: a sweep whose skip count is quietly climbing is a sweep
whose coverage is not what its success status implies.

!!! warning "Do not let the enqueue of the successor sit inside the per-item `try`"
    If the self-enqueue is inside a `try` that swallows exceptions, a failure to enqueue the
    successor is indistinguishable from a successful page and the chain ends silently. Keep the
    successor enqueue outside the loop and let it raise: a root retry re-runs one page, which the
    per-item keys make cheap.

---

## Bounding a runaway

A chain that advances is self-terminating; a chain whose cursor is *not* advancing is an infinite
job generator. The link ceiling in the example above exists for that case, and its role is worth
being precise about:

> A link ceiling is a **wall**, not the terminator. The short page is the terminator. Hitting the
> ceiling means the cursor is not advancing, which is a bug — so the ceiling should **fail
> loudly**, not return quietly.

Returning success at the ceiling converts an infinite loop into silent partial coverage, which is
strictly worse: you have lost both the work and the signal. Raise, so the job lands `failed` and
whatever watches failures tells you.

Set the ceiling well above the largest legitimate chain — `ceil(population / page_size)` with
generous headroom — so it never fires in normal operation. `MAX_LINKS = 1_000` at
`PAGE_SIZE = 500` admits half a million rows before it complains.

Two complementary bounds are worth knowing:

- **`max_pending`** caps the queued depth per actor, but read
  [the tripwire warning](#max_pending-on-a-chained-sweep) before putting one on a chained sweep.
- **`schedule_to_close`** on the successor caps the *wall-clock* life of a chain: past the
  deadline, the next link is not dispatched and lands `failed` with `DeadlineExceeded`. This is the
  right bound for "this sweep is pointless if it has not finished within six hours".

### `max_pending` on a chained sweep

`max_pending` counts `pending + scheduled` jobs for the actor — a `running` job does **not** count
— and raises `MaxPendingExceededError` at enqueue when the count is at the cap. On a chained sweep
that makes it a tripwire that kills the thing it protects:

A link that is *progressing normally* calls `enqueue` for its successor. If the actor's queue is
at the cap, that enqueue raises, the link fails, and **the walk ends**. Worse, it is sticky: the
retry re-walks the same page and enqueues into the same full queue, so it fails again for the same
reason. A backpressure signal has become a chain-terminating error, and the retry cannot clear it
because the retry is not what filled the queue.

Two ways out, both better than a cap on the chained actor:

- **Put `max_pending` on the leaf, not the root.** The leaf is the actor that can actually flood
  a queue (one job per item), and a rejected leaf enqueue is a per-item fault the isolation
  boundary already handles.
- **Let concurrency limit the chain instead of pending depth.** A chain is at most one pending
  successor per run; the depth that matters is the leaves'. Cap the leaves with
  `@actor(max_concurrent=...)`, a per-queue strict cap, or a `ConcurrencyReservation` — see
  [ops.md §3](ops.md#3-concurrency-process-actor-queue-fleet) for which of those is strict and
  which is a best-effort damper.

!!! note "A deep leaf backlog does not starve the root"
    Dispatch ranks candidates per actor (`ROW_NUMBER() OVER (PARTITION BY actor ...)`) and that
    rank is the *leading* sort key, ahead of priority — so every actor with pending work
    contributes to a dispatch round before any actor's second-ranked job is taken. Ten thousand
    pending leaves therefore cannot crowd out the one pending root that would advance the chain.
    This holds in both queue dispatch modes: `strict_fifo` keeps the per-actor partition and is
    FIFO only *within* an actor. What the per-actor ranking does **not** give you is global
    priority — a high-priority job of one actor loses to a low-priority job of another at a
    better rank. See [workers.md — Queue dispatch modes](workers.md#queue-dispatch-modes).

---

## Single-flight the chain

A cron trigger should start a chain only if one is not already in flight. What it must *not* rely
on is a guard whose lifetime is the root's lifetime, because a thin root outlives nothing.

| Approach | Verdict |
|---|---|
| `unique_for` + `identity_key` on the root | Correct for suppressing a duplicate *root*. Does **not** cover the descendants: the root succeeds in seconds and the window frees while the chain runs. |
| `singleton=True` on the root | Same blind spot, plus a sharper edge: three consecutive collisions **auto-disable the schedule permanently** (`cron_auto_disable_threshold=3`). See [ops.md — Cron and scheduled workloads](ops.md#cron-and-scheduled-workloads). |
| `unique_for` on the *successor* | **Wrong and chain-fatal.** A successor would dedup against its own `running` parent and the chain would end at link one. |
| An app-level run guard | **The robust option.** A row per sweep run with a status; the root refuses to start when a run is `active`, and a link marks it finished on the short page. |

The run guard is a handful of lines and it single-flights the *work* rather than the trigger:

```python
@actor(name="refresh_sweep_root", queue="cron")
async def refresh_sweep_root(payload: EmptyPayload, ctx: JobContext[EmptyPayload]) -> None:
    run_id = await begin_run_if_idle()  # your table; returns None if one is active
    if run_id is None:
        logger.info("refresh-sweep-already-running")
        return
    await ctx.jobs.enqueue(
        refresh_sweep,
        SweepPayload(run_id=run_id, cursor=None, link=0),
        idempotency_key=f"refresh-page:{run_id}:start",
    )
```

Give the run row a heartbeat or a start timestamp and let the root force-close a run that has been
`active` implausibly long. Otherwise a chain lost to an unrecoverable failure wedges the guard and
the sweep never starts again — the failure mode a bare "is a run active?" check introduces.

---

## When a sweep legitimately cannot chain

Not every sweep should chain, and the distinction is sharp enough to test.

A chain requires that **consuming a page moves the head of the walk**. Where that does not hold,
a successor re-reads the same head, finds the same rows, and the chain becomes a tight loop that
starves the very leaves it is waiting on.

The diagnostic case: a pass whose cursor is over rows **its own leaves produce**. An audit or
projection sweep that reads "the next unprocessed audit row" and whose leaves write new audit rows
has a head that only advances once a leaf has run. Self-enqueue immediately after fan-out and the
successor arrives before any leaf has finished, reads the unmoved head, fans out duplicates, and
enqueues *another* successor. Measured: roughly 1,488 links against one completed batch — the
chain spent the fleet's dispatch capacity on itself.

Such a pass is correctly **cron-paced**: it must let its leaves land before it looks again. The
distinction between this and a broken fixed-page sweep is not the shape of the code but the
answer to one question:

> After this pass returns, is the work it just enqueued *already excluded* from the next pass's
> query?

- **Yes** (the page's rows are now ineligible, or the cursor advanced past them): chain it. A
  successor sees new work, and cron-pacing it is the throughput bug described at the top.
- **No** (the next query returns the same rows until the leaves finish): do not chain. Cron pace
  it, and size the period above the expected leaf-completion time.

For the "no" case, make the pacing deliberate rather than accidental:

- **Page for the period, not for the tick.** Size the page to what the leaves can finish inside
  one period, so consecutive passes do not overlap on the same rows.
- **Exclude in-flight work in the query.** Skipping rows with a live job (or a claim timestamp)
  makes a pass idempotent against its own predecessor and lets you shorten the period safely.
- **Alert on the backlog, not the pass.** With a deliberately paced sweep, the pass succeeding
  says nothing about whether it is keeping up. Alert on the eligible-population depth and its
  trend — that is the signal a fixed-page sweep is missing, whether it is paced by choice or by
  accident.

---

## A swept run that reports success

One more failure worth recognising, because it inverts the usual relationship between a job's
status and reality: **100% of jobs reporting success while every run is failing.**

When a worker dies mid-job, its **lock lease** expires (`TASKQ_LOCK_LEASE`, 60 s by default) and
the leader's expired-lock sweep reclaims the row. Note the trigger is the lock lease, not
`heartbeat_timeout` — that per-job column is stored but not currently enforced by any sweep. What
the reclaim does depends on the retry budget:

```sql
SET status = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN 'pending'        -- redispatched, budget intact
        WHEN j.cancel_phase != 0
            THEN 'cancelled'
        ELSE 'crashed'            -- budget exhausted, terminal
    END
```

TaskQ's own terminal writes are fenced against the dead worker returning later — every one of them
carries `WHERE id = $1 AND status = 'running' AND locked_by_worker = $2`, so a reclaimed job
cannot be overwritten with `succeeded` by the worker that lost it. The write matches zero rows and
the job's status stands.

The failure appears when **application code** keeps its own run record and is not similarly
fenced. A sweep that writes "run succeeded" to its own table after the work, without checking that
it still owns the job, reports success for a run whose job was reclaimed and whose work never
finished. Every job row reads `succeeded` (they were redispatched and the retry did complete
something) while the application's runs are all failing, or vice versa.

Two defences:

- **Fence your own run writes the way TaskQ fences its own.** Include the job id and the attempt
  in the run row, and make the completion write conditional on them still matching.
- **Never report success from a path that did not do the work.** A helper that finds the run
  already terminal and returns success — rather than reporting that it found someone else's
  terminal state — manufactures the 100%-success signal. Return "not mine" and let it be visible.

And check `ctx.should_abort()` in long leaves: a reclaimed or cancelled job that keeps working is
the other half of this, where the side effects continue after the row has moved on
([cancellation.md](cancellation.md)).

---

## Checklist

Before a sweep goes near production:

- [ ] **A full page enqueues a successor immediately** — no "capped" log that returns, no
      dependence on the cron period for throughput
- [ ] **A short page enqueues nothing** — the chain terminates on data, not on a counter
- [ ] **Keyset cursor, not `OFFSET`** — ordered by a tuple ending in a unique column
- [ ] **The cursor column is not written by the walk** — or the walk pages an immutable snapshot
- [ ] **Root: `unique_for` + `identity_key`, no idempotency key**
- [ ] **Successor: idempotency key containing the cursor, no `unique_for`**
- [ ] **Leaf: idempotency key containing the run id and the item id**
- [ ] **No key component that can repeat across two enqueues that must both run** — no bare
      `updated_at` on a cancellable stage, no per-parent ordinals
- [ ] **Per-item `try`/`except` around each item**, with the successor enqueue *outside* it
- [ ] **A link ceiling that raises**, set well above the largest legitimate chain
- [ ] **No `max_pending` on the chained actor** — cap the leaves instead
- [ ] **Single-flight by a run guard**, with a staleness escape so a lost chain cannot wedge it
- [ ] **An alert on the eligible-population depth and trend**, not only on pass success
- [ ] **Application run-completion writes fenced** on job id and attempt

---

## Related documentation

- [ops.md §5 — Fan-out at scale](ops.md#5-fan-out-at-scale-chunks-cursors-idempotency) — the three
  chunking shapes, batch finalizers, the enqueue-API tradeoff table, idempotency-key discipline
- [ops.md — Pattern C](ops.md#pattern-c-app-level-run-accounting-finalize-sweep) — app-level run
  accounting when completion spans generations of jobs, which the run guard here builds on
- [ops.md §3 — Concurrency](ops.md#3-concurrency-process-actor-queue-fleet) — which caps are
  strict and which are best-effort dampers
- [ops.md §7 — Waiting politely](ops.md#7-waiting-politely-rate-limits-snooze-retryafter-retry-after)
  — `Snooze`, `RetryAfter`, and honoring `Retry-After`
- [jobs-clients.md — `idempotency_key`](jobs-clients.md#idempotency_key) — scopes, byte cap, the
  role table
- [cron.md](cron.md) — schedule registration, DST strategies, auto-disable
- [actors.md](actors.md) — `@actor` options, sub-job enqueue, `unique_for`
- [troubleshooting.md](troubleshooting.md) — symptom-indexed diagnosis, including the sweep
  symptoms above
