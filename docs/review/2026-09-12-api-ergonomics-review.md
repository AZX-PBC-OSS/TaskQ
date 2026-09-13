# TaskQ API ergonomics review — what should be impossible or loud, not merely documented

**Date:** 2026-09-12
**Basis:** one day building pipelines on TaskQ in a 48-actor consumer deployment; 20 confirmed
critical/high defects reducing to five recurring design errors.
**Source reviewed at:** `docs/sweep-patterns-and-ergonomics` @ `9157b31` (`feat: ops & adoption
guide, new_uuid export, max_retry_backoff wiring (#119)`).
**Scope:** API/ergonomics only. Documentation is being improved separately and is explicitly *not*
the remedy proposed here.

---

## 0. The framing that matters

Every single defect class below is **already documented**, most of them in the `ops.md` guide that
landed in the tip commit. Its §5 "Idempotency-key discipline" list opens with:

> **Idempotency-key discipline** — every rule below was learned from a production incident:

…and then states, verbatim, both of the mistakes that cost this deployment the most:

- "**Keys are status-blind.** A terminal-`failed` job's key absorbs every re-post onto the dead
  job — your post-fix rerun of a failed batch will silently no-op."
- "**No stable key on a self-continuation successor.** A continuation job carrying the same key as
  its (now-`succeeded`) predecessor collides with it and is silently dropped."

Both are also in §10 "The footgun index" (rows "Stable key on a self-continuation successor" and
"Key missing a payload dimension") and in §11's adoption checklist.

> **A note on citations.** `docs/guides/ops.md` is being actively edited in this worktree by a
> concurrent docs change, so references to it below are by **section heading**, not line number.
> Citations into `src/` are by line and were verified against `9157b31`.

**That is the finding.** These are not undocumented traps; they are traps documented to the
highest standard I have seen in an OSS queue, that *still* bit a careful adopter twice each, in
one day, with the docs open. The docs have been exhausted as an instrument. What remains is
changing the API so the mistake is either impossible to express or loud when expressed.

The proposals below are therefore filtered by one test: **does it convert a silent wrong outcome
into a loud one, using a mechanism TaskQ already uses elsewhere?** TaskQ has four such mechanisms
already, and every DO item reuses one rather than inventing anything:

| Mechanism | Existing instance | Cited at |
|---|---|---|
| Import-time `@actor` warning for incoherent config | `actor-config-indefinite-no-budget` | `src/taskq/actor.py:708-733` |
| Once-per-actor enqueue-time warning for a silently-inert parameter | `actor_config_unique_for_ignored` | `src/taskq/client/_jobs.py:206-225` |
| Aggregated startup refusal with a remediation hint | `ActorConfigDriftList` | `src/taskq/exceptions.py:358-369` |
| Typed error instead of an ambiguous `None` | `EmptyBatchError`, `on_empty="error"` default | `src/taskq/batch.py:392`, `exceptions.py:637` |

---

## 1. Executive summary — the three with the best harm-prevented/cost ratio

### DO #1 — `JobHandle.deduplicated_onto_terminal` + a warning when a dedup resolves onto a terminal row
The whole of mistake 1 and half of mistake 2 are one bug: an enqueue silently returns a handle to
a **terminal** predecessor and the chain stops. The status of that predecessor is *already in the
handle* at zero extra cost (`JobHandle._row`, `_handle.py:81`, exposed as `.row`), and
`TERMINAL_STATUSES` is already public (`src/taskq/__init__.py:123`). So the fix is a property plus
one `logger.warning` at the existing dedup log site (`_enqueue.py:335-346`). Purely additive, ~30
lines, no new failure modes. **This is the single highest-value change in this document.**

### DO #2 — `resolved-capacity` startup log + `taskq doctor`
Mistake 3 measured: raised the tier cap and the per-actor caps, got zero change, because a
workgroup child's `--max-concurrency` still bound at 4. Verified: **no worker logs its effective
binding concurrency anywhere**, per actor or per queue; queue caps registered from
`queues.max_concurrent` are logged *nowhere* (`_bootstrap.py:890-900` registers them silently).
Every input is already resolved in one place at startup. One aggregated log line per actor naming
the *binding* layer, plus a `doctor` command, and this class dies. Purely additive.

### DO #3 — make per-item isolation a library primitive (`ctx.for_each`)
Mistake 9 measured: one `ValidationError` killed an enumeration for every item that hour, twice.
`ops.md` §6's failure-doctrine table already prescribes the remedy as *doctrine* ("one unreadable item must never fail the
whole page/run") but there is no API for it, so every one of 48 actors hand-rolls the try/except
and any one of them can forget. This is the only proposal here that adds a genuinely new
primitive; I recommend it because the doctrine is already settled, so the abstraction is not
speculative.

**Everything else I would not do now, and §3 says why.** In particular I recommend **against** a
`@sweep` decorator, against `enqueue_successor()`, and against exempting successors from
`max_pending` — all three are attractive and all three are wrong.

---

## 2. Proposals

### P1 — Surface the dedup target's status; warn when it is terminal

**Verdict: DO. Compatibility: purely additive.**

#### Problem

`ON CONFLICT` carries no status predicate. `src/taskq/backend/_sql_templates.py:537-538`:

```sql
ON CONFLICT (idempotency_scope, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
```

and the follow-up lookup is equally status-blind (`_sql_templates.py:557`):

```sql
enqueue_select_by_key=f"""\
SELECT * FROM "{s}".jobs WHERE idempotency_scope = $1 AND idempotency_key = $2""",
```

The horizon is retention, and the default is **30 days** (`DEFAULT_PRUNE_RETENTION:
timedelta(days=30)`, `src/taskq/constants.py:134`) — so a successor colliding with its own
`succeeded` predecessor is dropped for a month, not for a sweep interval.

The measured consequence: chain stops dead, no error, job never created, twice. And note the
irony — `ops.md` §5 Pattern A, the *recommended* shape, ships this exact form:

```python
        await ctx.jobs.enqueue(
            sync_page,
            SyncPagePayload(run_id=payload.run_id, cursor=next_cursor),
            idempotency_key=f"sync:{payload.run_id}:{next_cursor}",
        )
```

That is correct *only* because `next_cursor` is a fresh dimension per link. A reader who keys on
anything that does not move per link — a stage name, a run id, an `updated_at` that never advances
because the handler never ran (mistake 2's cancelled-job case) — gets silence. The API cannot tell
the two apart, and neither can the reader.

`was_existing` exists (`_handle.py:83`) but is inadequate for three concrete reasons:
1. it does not distinguish *active* dedup (correct, benign — a concurrent duplicate) from
   *terminal* dedup (almost always a bug);
2. it is a bare `bool` on a handle most callers discard — `await ctx.jobs.enqueue(...)` with no
   assignment is the idiom in the docs' own examples;
3. nothing logs when it is `True` at a level anyone alerts on. The existing dedup log
   (`_enqueue.py:335-346`) is `logger.info`.

#### Proposed API

Two additions. First, on `JobHandle` — free, because the row is already there:

```python
# src/taskq/client/_handle.py
@property
def deduplicated_onto_terminal(self) -> bool:
    """``True`` when this enqueue deduplicated onto a job that had already
    reached a terminal status.

    Almost always a bug: the work this call asked for will NOT run. A
    self-continuation successor keyed on a value that does not advance per
    link, or a re-post after a terminal failure, both land here. Deduplicating
    onto an ACTIVE job (pending/scheduled/running) is the benign case this
    property excludes.
    """
    return self.was_existing and self._row.status in TERMINAL_STATUSES
```

Second, at the existing dedup log site in `_enqueue.py`, split the log by status — reusing the
`_jobs.py:206` once-per-actor suppression so a legitimate high-volume re-post does not flood:

```python
    else:
        if row.status in TERMINAL_STATUSES:
            logger.warning(
                "enqueue_deduplicated_onto_terminal",
                kind="enqueue_deduplicated_onto_terminal",
                job_id=str(row.id), actor=row.actor,
                existing_status=row.status,
                idempotency_key=row.idempotency_key,
                idempotency_scope=row.idempotency_scope,
                reason=(
                    "this enqueue returned an existing job that is already terminal; "
                    "the requested work will NOT run. If this is a continuation or a "
                    "re-post, the key needs an axis that advances per attempt/link "
                    "(see ops.md idempotency-key discipline)."
                ),
            )
        else:
            logger.info("enqueue_deduplicated", ...)  # unchanged
```

#### Before / after

```python
# BEFORE — silent. Chain stops; nothing distinguishes this from success.
await ctx.jobs.enqueue(sync_page, next_payload, idempotency_key=f"sync:{run_id}:{stage}")

# AFTER — same call, now emits enqueue_deduplicated_onto_terminal at WARNING.
# Callers that want to fail loudly can also assert:
h = await ctx.jobs.enqueue(sync_page, next_payload, idempotency_key=...)
if h.deduplicated_onto_terminal:
    raise RuntimeError(f"continuation collapsed onto terminal {h.job_id}")
```

#### Cost, risks, and what it takes away

- **Implementation:** small. One property, one branch at an existing log site. `TERMINAL_STATUSES`
  and `row.status` are both already in scope on both paths. The batch paths
  (`_jobs.py:855`, `:906`) construct handles the same way and inherit the property for free,
  though `enqueue_batch`'s `RETURNING` list already includes `status`
  (`_sql_templates.py:620`) so the warning can be wired there too.
- **New failure modes:** none. No behaviour changes; only an added property and a log level
  change on a branch that already logged.
- **Takes away from a correct caller:** nothing, except log volume — and only on the branch that
  is nearly always a defect. A caller who *deliberately* re-posts onto terminal rows (a
  "has this ever run?" probe) would see warnings; the once-per-actor suppression bounds that, and
  such a caller should arguably be using `client.get()` instead.
- **Why not a status predicate in the `ON CONFLICT` itself:** because it cannot be one. A partial
  unique index predicate must be `IMMUTABLE`, which is exactly the reasoning already recorded for
  `unique_for` at `_enqueue.py:143-152`. Adding a status predicate would also silently change
  dedup semantics for every existing caller — breaking, and in the dangerous direction (duplicate
  work rather than lost work). **Do not do that.** The docs also already refused the adjacent ask
  (an idempotency TTL) with a sound rationale at `jobs-clients.md:200-206`; this proposal is
  deliberately compatible with that refusal — it changes no dedup semantics at all.

---

### P2 — `resolved-capacity` startup log + `taskq doctor`

**Verdict: DO. Compatibility: purely additive.**

#### Problem

Mistake 3 is real and the mechanism is confirmed. There are four layers, and the *binding* one is
invisible:

| Layer | Semantics | Enforced at | Live? |
|---|---|---|---|
| `TASKQ_MAX_CONCURRENCY` | per **process** | `local_queue` bound + N consumer tasks (`_bootstrap.py:1002-1004`, `:1121`) | restart |
| workgroup child `max_concurrency` | per **child process** | passed as `--max-concurrency` (`workgroup.py:136`) | restart |
| `actor_config.max_concurrent` | per actor, fleet-wide, **best-effort** | dispatch CTE (`_dispatch_sql.py:85-94`, `:149-164`) | **live, every round** |
| `queues.max_concurrent` | per queue, fleet-wide, **strict** | `ConcurrencyReservation` slots (`reservation.py:41-56`) | **restart** |

The over-admission is not a subtlety to be inferred — the SQL says it
(`_dispatch_sql.py:53-65`):

```sql
-- Two dispatchers running concurrently each see the same in_flight, each
-- admit up to `max_concurrent - in_flight`, and lock DISJOINT pending rows
-- -- so SKIP LOCKED does not serialize them and both succeed. The
-- over-dispatch bound is (num_producers - 1) * max_concurrent per round,
-- ... `max_concurrent` is therefore a per-round admission
-- damper, NOT a hard fleet-wide cap.
```

What is missing is any report of the **resolved** value. Verified absent: no emission in
`_bootstrap.py` or `startup.py` computes an effective cap. The nearest misses are
`producer-loop-start` (`run.py:194-201`), which logs only `max_concurrency`, and
`ratelimit-actor-primitives-registered` (`_bootstrap.py:607-613`), which logs reservation *names*
but not slot counts. Queue caps are registered silently (`_bootstrap.py:890-900`).

Two further traps in the same family, both worth catching in the same pass:
- **A missing `actor_config` row means the actor never dispatches at all** — the dispatch CTE draws
  candidates `FROM actor_config` (`_dispatch_sql.py:92`). Not "uncapped": *zero*. The CLI already
  knows to say so (`cli.py:906-923`).
- **A stored `NULL max_concurrent` is uncapped, and can never fall back to the literal** —
  `_effective_capacity`, `cli.py:893-895`. `max_pending`/`result_ttl` behave the *opposite* way.

#### Proposed API

**(a) One aggregated startup log line per actor** (reusing the `_emit_*_startup_warnings` pattern
in `_bootstrap.py`, and the `effective`+source shape already implemented by
`_effective_capacity`, `cli.py:884-898`):

```python
def _emit_resolved_capacity_startup_log(
    settings: WorkerSettings,
    actor_registry: Mapping[str, ActorRef[Any, Any]],
    stored: Mapping[str, ActorConfigRow],
    rl_registry: RateLimitRegistry,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Log the effective concurrency ceiling per actor and name the BINDING layer."""
```

emitting, per actor, one event:

```
resolved-capacity actor=fetch_resume process_limit=4 actor_cap=20 queue_cap=8
                  reservation_slots=2 effective=2 binding="reservation:pdfium"
                  binding_is_strict=true
```

The `binding` field is the whole point: it is the `min()` argument that won, named. In the measured
incident it would have read `binding="process"` with `process_limit=4`, and the day would have
ended differently.

**(b) `taskq doctor`** — a read-only aggregate of what the existing commands already compute
separately. There is no `doctor`/`diagnose`/`config` command today (verified against `cli.py:67-99`);
`taskq actor-config diff` (`cli.py:961-968`) is 80% of the per-actor half already and should be
called from it rather than reimplemented. `doctor` adds the three checks that have no home:

1. **queue-cap staleness** — no diff exists for queue caps, and they need a restart, so nothing
   today tells you a running worker is enforcing a stale slot count. Compare
   `queues.max_concurrent` against the live `reservation_slots` row count per bucket.
2. **actors with no `actor_config` row** — i.e. actors that will never dispatch.
3. **incoherent-by-construction combinations** — the two cheap ones: `unique_for` declared on an
   actor with no `identity_key` reaching enqueue (today: a once-per-process log,
   `_jobs.py:206-225`), and `fairness_key` set on a `strict_fifo` queue (today: documented inert at
   `jobs-clients.md:162`, signalled nowhere).

#### Before / after

```console
# BEFORE — raise both caps, observe nothing, guess for an afternoon.
$ taskq actor-config set fetch_resume --max-concurrent 20
$ taskq queues set-max-concurrent heavy --max-concurrent 8
# (no change in throughput; nothing in any log explains why)

# AFTER
$ taskq doctor
actors
  fetch_resume     effective=2   binding=reservation:pdfium (strict)   [actor_cap=20 ignored]
  derive_links     effective=0   binding=NO ACTOR_CONFIG ROW — never dispatches
queues
  heavy            stored=8  live_slots=4  STALE — workers started before the change; restart
config
  WARN fairness_key passed for actor bulk_tag on strict_fifo queue 'bulk' — inert
```

#### Cost, risks, and what it takes away

- **Implementation:** moderate but contained, and mostly assembly. Every input already exists in
  one process at one moment: `settings.max_concurrency`, the `stored` mapping already fetched by
  `sync_actor_config` (`startup.py:120-122`), the `cap_rows` already fetched at
  `_bootstrap.py:876-882`, and the registry that already holds every reservation and its slot count
  (`registry.py:350-368`). `doctor` is a new command but a thin one — it should *call*
  `actor-config diff`'s resolution helper, not fork it.
- **New failure modes:** one to avoid deliberately. **`doctor` must never exit non-zero on a
  warning** and the startup log **must never refuse to boot**. A "per-actor sum exceeds a strict
  ceiling" configuration is frequently *intentional* (oversubscription with a strict backstop is
  the documented pattern for `ConcurrencyReservation`), so refusing to start on it would break
  correct deployments. This is the same discipline TaskQ already applies to
  `assignment_check`-style probes: report, never gate. Refusal is reserved for *structural* drift,
  which `ActorConfigDriftList` already owns.
- **Takes away:** nothing. Log volume is one line per actor at startup; at 48 actors that is 48
  lines once per boot, which is the right price for this class of defect.
- **Note on the user's hypothesis "refuse to start on an incoherent configuration":** I
  recommend against it, for the reason above. The loudness belongs in `doctor` and the log, not in
  a boot gate.

---

### P3 — `ctx.for_each(...)`: per-item isolation by construction

**Verdict: DO (smallest viable version only). Compatibility: purely additive.**

#### Problem

Mistake 9, measured: one `ValidationError` killed an enumeration for every item that hour, twice.
The doctrine is already settled in the docs — `ops.md` §6, failure-doctrine table:

> | **Per-item** faults inside a fan-out (one deleted file in a 500-item page) | per-item fault, skip and continue — **not** a job failure | one unreadable item must never fail the whole page/run; record it and move on |

But there is no API for it, so the correctness of 48 actors rests on each one remembering a
try/except. Note also that the enqueue side is *already* protected in one direction and not the
other: `PartialBatchError` (`exceptions.py:448-455`) isolates per-item *enqueue* failures with
`failed_items: list[tuple[int, Exception]]`, but only on the autonomous fallback path —
`enqueue_batch` proper binds every item into one `unnest` INSERT
(`_enqueuer.py:316-318`), all-or-nothing. So the library already has the *vocabulary* for
per-item outcomes; it just does not offer it for processing.

#### Proposed API

Deliberately minimal — a loop with a result record, not a framework:

```python
# src/taskq/context.py (on JobContext)
async def for_each[T](
    self,
    items: Iterable[T],
    fn: Callable[[T], Awaitable[None]],
    *,
    on_error: Literal["collect", "raise"] = "collect",
    max_failures: int | None = None,
) -> ItemOutcomes[T]:
    """Apply *fn* to each item, isolating per-item failures.

    ``collect`` (default) records the failure and continues; the returned
    ``ItemOutcomes`` carries ``failed: list[tuple[T, Exception]]``. A
    ``max_failures`` ceiling re-raises once exceeded, so a systemic fault
    (auth revoked, schema change) still fails the job loudly instead of
    logging 500 identical errors and reporting success.
    """
```

`ItemOutcomes` is a small frozen model: `succeeded: int`, `failed: list[tuple[T, Exception]]`,
`total: int` — deliberately the same shape as `PartialBatchError`'s fields so the two read alike.

The `max_failures` ceiling is the part that earns this proposal. "Skip and continue" without a
ceiling converts a total outage into a green run that processed nothing — which is mistake 6's
failure mode arriving by a different road. Default `None` preserves today's documented doctrine;
setting it is how a caller says "isolated faults are expected, a systemic one is not."

#### Before / after

```python
# BEFORE — one bad row kills the page; the other 499 items never run, and
# the next attempt re-reads the same page and dies the same way.
for row in rows:
    await handle_row(row)

# AFTER
outcomes = await ctx.for_each(rows, handle_row, max_failures=25)
if outcomes.failed:
    log.warning("page-partial", failed=len(outcomes.failed), total=outcomes.total)
```

#### Cost, risks, and what it takes away

- **Implementation:** small, pure Python on `JobContext`, no SQL, no new tables.
- **New failure modes:** the real risk is **encouraging swallowed errors**. Mitigated by
  `max_failures`, and by returning a value the caller must inspect rather than logging and
  discarding. It must not be `async`-concurrent in v1 (no `gather`, no semaphore) — concurrency
  here would interact with per-actor caps and `ctx.should_abort()` in ways that need their own
  design. Sequential only.
- **Takes away:** nothing; a hand-rolled try/except keeps working.
- **Honest caveat:** this is the one proposal where "a wrong abstraction is worse than none"
  applies, and it is why I scoped it to a loop rather than a fan-out helper. If the maintainer
  prefers zero new primitives, the fallback is to leave it to docs — the cost of that choice is
  that it stays a per-actor discipline, which is exactly how it failed twice.

---

### P4 — Distinguish "no open run" from "swept / cancelled / never existed"

**Verdict: DO LATER (TaskQ's own instance of it), and it is NOT the consumer's bug to fix in TaskQ.
Compatibility: additive (new method), or additive-with-deprecation if the existing return type
changes.**

#### Problem

The user's report is about a consumer function (`open_running_run`-style) returning `None` for four
distinct facts. **Confirmed: no such function exists in TaskQ** — no `runs` table, no `run_id`
column, no `open_running_run` anywhere in `src/`. TaskQ has no "run" concept; the nearest thing is
`batches` (`01.00.05_01_pre_batches.sql:5-21`). So this defect is genuinely consumer-side and
cannot be fixed by a TaskQ change. `ops.md` §5 even documents "Pattern C — app-level run
accounting" as user code precisely because TaskQ has no run concept.

**But TaskQ has the same defect in its own API**, and that part is actionable:

```python
# src/taskq/backend/_batch_sql.py:285-290
async def get_batch(conn, sql, batch_id) -> BatchRow | None:
    """Fetch a single batch row by ID, or ``None`` if not found."""
```

That `None` conflates at least three facts: never existed; existed and was pruned
(`prune_old_batches`, `_batch_sql.py:437-446`); and *created without a `failure_policy` or
finalizer, so no row was ever written* (`_jobs.py:579`). `JobsClient.get` has the same shape
(`_jobs.py:1093-1096`), where `None` also silently covers "pruned to `jobs_archive`". And
`increment_batch_failures` encodes absence as a *value* — `(0, None, 0)` (`_batch_sql.py:304`).

Worth crediting: TaskQ already got this right once, in the newest part of the API.
`wait_for_batch` defaults to `on_empty="error"` and raises `EmptyBatchError` rather than returning
an ambiguous empty status (`batch.py:392-393`, `exceptions.py:637-644`), and it raises
`BatchAbortedError` for the aborted case. That is the correct precedent; it simply has not been
applied to the older read paths.

#### Proposed API

Do **not** change `get_batch`'s signature. Add a sibling that cannot be ambiguous:

```python
type BatchLookup = BatchFound | BatchPruned | BatchNeverExisted


async def lookup_batch(self, batch_id: UUID) -> BatchLookup: ...
```

`BatchPruned` is distinguishable in principle (a terminal job in `jobs_archive` carrying that
`metadata.batch_id`), which is what makes the three-way split honest rather than cosmetic.

#### Cost, risks, verdict reasoning

- **Cost:** moderate, and higher than it looks — distinguishing "pruned" from "never existed"
  requires an archive probe, i.e. a second query on a cold path.
- **Why DO LATER:** the measured harm was entirely in consumer code, over a consumer-owned table.
  Fixing TaskQ's `get_batch` would not have prevented a single one of the 20 defects. It is the
  right change on the merits and the wrong change to prioritise against P1–P3.
- **What I would do now instead, at near-zero cost:** document the ambiguity on the two existing
  methods. That is a docs change and belongs to the concurrent docs work, not here.

---

### P5 — `max_pending` and successor enqueues

**Verdict: DO NOT exempt successors. DO make the error self-describing. Compatibility of the
recommended part: purely additive.**

#### Problem, and a correction

The user's account: setting `max_pending` on a chained sweep meant a progressing link's successor
enqueue raised `MaxPendingExceededError`, ending the walk, and the retry re-walked into the same
full queue, so the failure was sticky.

The mechanism is confirmed. `_enqueue.py:222-239` checks pending+scheduled depth before the INSERT
and raises; the count is `status IN ('pending','scheduled')` (`_sql_templates.py:554-556`).
`MaxPendingExceededError` is a `BackpressureError` (`exceptions.py:107`) and appears in **no**
retryable set — its docstring is explicit (`exceptions.py:112-114`):

> The caller decides whether to retry, fail, or wait; the library does not block on capacity.

So raised inside an actor body it reaches the generic classifier and burns attempts on the actor's
static policy.

**But the three fixes the user floats are all worse than the status quo:**

- **Exempting successor enqueues** would be a correctness hole, not an ergonomic fix. `max_pending`
  is the *only* enqueue-side defence against unbounded queue growth, and a self-continuing chain is
  precisely the workload that can grow without bound. An exemption means the one shape that can run
  away is the one shape that is not capped. Note `enqueue_batch_streaming` and
  `enqueue_batch_fast` already do not enforce it (`ops.md` §5, batch-path comparison table) and
  that is documented as a footgun (`ops.md` §10), not a feature to extend.
- **Making the error retryable-by-construction** is already available and better expressed by the
  caller: `raise Snooze(delay)` (`exceptions.py:240`) reschedules **without consuming budget** —
  `attempt` unchanged, `max_attempts` bumped by one (`retries.md:443`). A library-imposed retry
  kind would also silently override an actor whose author deliberately wants shedding to be
  terminal.
- **Shedding differently** (dropping the oldest, or blocking) changes durability semantics and
  needs a broker TaskQ deliberately does not have.

#### What I would actually do

The genuine gap is that the error does not tell the caller the one thing it knows: that this is a
*wait*, not a *failure*. Add a hint, following the `hint` convention already on
`ActorConfigDriftError` (`exceptions.py:339`):

```python
class MaxPendingExceededError(BackpressureError):
    hint = (
        "Backpressure, not a failure. Inside an actor, prefer "
        "`raise Snooze(delay)` — it reschedules without consuming retry "
        "budget — over letting this reach the retry classifier."
    )
```

Roughly five lines, zero behaviour change, and it puts the correct primitive in front of the person
reading the traceback at the moment they need it. Cost: none. Risk: none.

---

### P6 — Rate limits and the failure budget

**Verdict: DO NOT. The premise is refuted; the capability already ships and is documented.**

This is the correction I am most confident about, so I will be precise.

1. **`advertised_retry_after` does not exist in TaskQ.** Zero matches across `src/`, `docs/`,
   `tests/`. It is a **consumer-authored** `RetryClassifierHook` in TAStack
   (`backend/packages/ta-worker/src/ta_worker/retry.py:67`).
2. **A rate-limit outcome is already a distinct kind, and the classifier already honours it.**
   `RetryOverride` carries **both** `kind` and `delay` (`retry.py:209-210`), and the
   `max_attempts` check is *guarded by* `effective_kind == "transient"` (`retry.py:302-303`) — for
   `indefinite` it is never evaluated (`retry.py:312-318`). `RetryOverride`'s own docstring names
   this exact use case (`retry.py:196-201`): "an HTTP 429 response goes `indefinite` while a 404
   response on the same exception type goes `non_retryable`."
3. **Better still, two budget-free primitives ship and are documented.** `Snooze` and
   `RetryAfter(delay, consume_budget=False)` bypass classification entirely
   (`_handlers.py:783`, `:800`), both implemented as `max_attempts = j.max_attempts + 1`
   (`_sql_templates.py:329` in `mark_snoozed`, `:454` in `mark_retry_after_consume_false`).
   `docs/guides/retries.md:461-470` documents `RetryAfter` with the
   rate-limit case as the worked example, and `ops.md` §6 prescribes it in the failure-doctrine
   table.
4. **The measured incident was a consumer bug against an API that already supported the fix, and it
   has since been fixed.** The original consumer classifier returned
   `RetryOverride(delay=timedelta(seconds=exc.retry_after_s))` with **no `kind`** — delay honoured,
   budget still spent, job dead after 4 sustained 429s. The current version sets
   `kind="indefinite" if attempt < MAX_RATE_LIMITED_ATTEMPTS else None` with a ceiling of 40
   (`ta_worker/retry.py:47`, `:104`).

So there is nothing to build. **The one thing I would change is discoverability**, and it is
one line, not an API: the `RetryOverride.delay` field docstring should say that `delay` alone does
not spare the attempt budget and point at `kind="indefinite"` / `RetryAfter`. That is the exact
mistake that was made, and the field's own docstring is where the reader was standing.

A distinct `rate_limited` job status — the user's literal suggestion — I recommend **against**:
`job_status` deliberately excludes it (`01.00.00_01_pre_initial.sql:51-53` — "`awaiting_resource`
is intentionally NOT in this enum"), the reservation path already lands `scheduled` +
`metadata.awaiting`, and `AttemptOutcome` already carries `rate_limit_denied`
(`_protocol.py:161`). Adding an enum member is a breaking migration for every consumer that
switches on status, to express something already expressible.

---

### P7 — Version components that do not version

**Verdict: DO NOT build key-freshness detection. Partly covered by P1; the rest cannot be done
soundly.**

Mistake 2 has two halves and they need different answers.

**Half A — a stage key versioned on `updated_at` never moves when a job is CANCELLED, because the
handler never ran.** Confirmed at the mechanism level: for a `pending`/`scheduled` job, cancel goes
straight to terminal in SQL (`_cancel_bulk.py:77-87`) and the handler never runs; a cooperative
cancel writes **no `error_class` at all** (`_sql_templates.py:290-300`), so operator-cancel is not
even distinguishable on the job row. And there is **no cancel hook** — nothing registers a user
callback on cancel; the only mechanism is polling `ctx.cancellation_requested`
(`context.py:72-91`). So a consumer genuinely cannot update bookkeeping on the
cancelled-while-pending path. 3 files wedged 3.5 days across ~330 passes with a green sweep is the
predictable result.

The honest assessment: **TaskQ cannot detect this.** "Did the generation component of this key
advance since the last attempt?" requires TaskQ to know which part of the caller's key string is a
generation, and to have stored the previous one. Both are outside what a payload-blind queue can
see — `ops.md` §5 says so ("TaskQ cannot see payload contents"). Any heuristic here would produce
false positives on legitimately-stable keys, which is worse than silence.

What *does* help, and is already proposed: **P1**. The cancelled-predecessor case surfaces as
`enqueue_deduplicated_onto_terminal` with `existing_status="cancelled"`, which names the problem
precisely at the moment it occurs. That is the reachable 80%.

**Half B — ordinal keys (`{run_id}:{index}`) collide across links.** This is the payload-blindness
rule already at `ops.md` §5's payload-dimension bullet, and it is a caller error P1 also catches (the second link's
`index=0` collides with the first link's terminal `index=0`).

**A `parent_job_id` / chain-linkage column** — worth naming since it is the structural fix one
might reach for. There is no parent/child linkage, no depth counter, no chain id anywhere in the
schema. `batches.originating_actor` exists but is *"Reserved for future use — currently always
`None`"* (`jobs-clients.md:818`). Adding real linkage would enable a genuine "this successor
collided with its own parent" check. I am **not** proposing it: it is a migration plus a write on
every enqueue, to catch a class P1 already catches by status, and the promised
`originating_actor` work should land first and be evaluated on its own terms.

---

### P8 — A first-class sweep/fan-out abstraction

**Verdict: DO NOT. This is the proposal I most want to reject, and the user's own skepticism is
correct.**

Mistake 7 is real and the harm is large — hand-rolled page/fan-out/recurse everywhere, so most
sweeps end up cron-paced (one at 250/hour for ~10 minutes of work; another ~10 days for a ~1.3h
job). But a `@sweep` decorator is the wrong remedy, for four reasons, one of which the user
supplied and the source confirms.

1. **The non-chaining case is legitimate and the abstraction would break it.** A pass whose cursor
   *is* the audit rows its own leaves write cannot chain: a successor re-reads an unmoved head and
   starves the leaves — measured ~1,488 links against one completed batch. A `@sweep` decorator
   whose contract is "return the next cursor and I enqueue the successor" is *exactly* the shape
   that produces that pathology, and it would produce it by default, invisibly. Encoding the
   chaining assumption into a decorator makes the worst measured outcome the path of least
   resistance.
2. **"Sweep" already means something else in TaskQ, and it is load-bearing.** There are ~10
   internal maintenance sweeps, leader-gated, documented as Sweeps 1–5
   (`_sweeps.py:1-7`, `_leader_shared.py`). A `@sweep` decorator for user pipelines would
   permanently collide with the vocabulary of the operational docs, the metrics, and the
   troubleshooting guide. If anything like this ever ships it must not be called `sweep`.
3. **The composable pieces already exist and the gap is genuinely documentation.** `Snooze` for
   self-continuation (`exceptions.py:240`), `enqueue_batch(..., finalizer=...)`
   (`_jobs.py:428`), `wait_for_batch` with `EmptyBatchError`/`BatchAbortedError`
   (`batch.py:385`), `batches.failure_threshold` for abort-on-consecutive-failures
   (`batch_policy.py:39-59`), tags for group operations. `ops.md` §5 already documents three
   named shapes (Pattern A cursor chain, Pattern B fan-out+finalizer, Pattern C app-level run
   accounting). A concurrent agent is writing `docs/guides/sweeps.md` right now. **Let that land
   and measure whether the pacing problem persists** before adding a primitive whose wrong version
   is worse than none.
4. **The measured harm was pacing, and pacing is not what an abstraction fixes.** A cron-paced
   sweep at 250/hour is a *configuration* outcome (cron cadence chosen instead of chaining), and
   the fix is knowing that chaining is available and when it is safe — a docs and `doctor`
   concern, not a decorator.

**What I would do instead, if the pacing problem survives the new guide:** ship a *narrow*
`PagedWalk` helper that owns exactly one thing — the successor enqueue with a
provably-advancing cursor — and **refuses** to enqueue when the cursor has not advanced:

```python
async def enqueue_next_page(
    ctx, actor_ref, *, run_id: str, cursor: C, previous: C
) -> JobHandle | None:
    """Enqueue the successor page. Returns None (and logs) when `cursor == previous`,
    rather than enqueueing a link that will re-read an unmoved head."""
```

That is the one invariant the hand-rolled versions get wrong, it composes with everything above,
and it makes the ~1,488-link pathology structurally impossible rather than merely documented. But
it is a follow-up to the docs work, not a substitute for it — **DO LATER at best.**

---

### P9 — `enqueue_successor()` / a `role=` parameter

**Verdict: DO NOT.**

The user's own first hypothesis, and I think it is the wrong shape — worth saying plainly since it
is the most natural reading of mistake 1.

- **`role=` is a lie the API cannot verify.** A `role="successor"` parameter that merely selects
  different dedup behaviour is an unvalidated assertion; nothing stops a leaf from claiming it, and
  the failure mode of a wrong `role=` is silent, which is the disease.
- **`enqueue_successor()` as a distinct method** would need to differ from `enqueue()` in its dedup
  semantics to be worth having — and the only useful difference is "ignore terminal predecessors",
  which is either a status-predicated `ON CONFLICT` (impossible, non-`IMMUTABLE` predicate,
  `_enqueue.py:143-152`) or a pre-INSERT status probe (a new TOCTOU window, and the serialization
  fix for that is the `unique_for` advisory lock, whose cost and rationale are documented at
  `_enqueue.py:128-170`). So the honest version of `enqueue_successor()` is "enqueue with a fresh
  key", which callers can already write and which `new_uuid` (`_ids.py:121`, exported in the tip
  commit) already serves.
- **The surface cannot afford it.** `@actor` is at 21 keywords and `enqueue()` at 15 plus 2
  positionals. Adding a `role=` whose only effect is to change the meaning of another parameter
  makes the combination space worse, and nothing today validates combinations at all.
- **Making a key mandatory-or-explicitly-none** (the user's other option) is **breaking** for every
  existing call site — and it would not have prevented either incident, because in both the key was
  supplied deliberately. It just was not fresh.

P1 gets the benefit — a loud signal on the exact wrong outcome — without asserting a role the
library cannot check.

---

## 3. Rejected, with reasons

| # | Considered | Rejected because |
|---|---|---|
| R1 | Status predicate in `ON CONFLICT` / dedup only against active rows | Index predicate must be `IMMUTABLE` (`_enqueue.py:143-152`); and it silently flips every existing caller from lost-work to duplicate-work. **Breaking, in the dangerous direction.** |
| R2 | `idempotency_ttl` parameter | Already refused with a sound rationale (`jobs-clients.md:200-206`): a sliding window cannot be one static unique index; Oban/River both trade away the atomic `ON CONFLICT`. Scope is the sanctioned escape hatch. |
| R3 | `enqueue_successor()` / `role=` | §P9. Unverifiable assertion; the honest version is "fresh key", already available via `new_uuid`. |
| R4 | Mandatory-or-explicitly-none idempotency key | Breaking for every call site, and would not have caught either incident — the keys were deliberate but stale. |
| R5 | `@sweep` decorator / `PagedWalk` framework | §P8. Would make the ~1,488-link starvation the default path; collides with TaskQ's own load-bearing "sweep" vocabulary; docs work is in flight and unmeasured. |
| R6 | Exempt successor enqueues from `max_pending` | §P5. Removes the only backpressure from the one workload that can run away unboundedly. |
| R7 | Make `MaxPendingExceededError` retryable by construction | §P5. `Snooze` already does this better and leaves the choice with the actor author. |
| R8 | Distinct `rate_limited` job status | §P6. Deliberately excluded from the enum (`01.00.00_01_pre_initial.sql:51-53`); already expressible as `scheduled` + `metadata.awaiting` and `AttemptOutcome.rate_limit_denied`. Breaking migration for zero new capability. |
| R9 | Key-freshness / generation-advance detection | §P7. Requires TaskQ to parse the caller's key and remember the previous one; payload-blind by design. False positives on stable keys are worse than silence. P1 covers the reachable part. |
| R10 | Refuse to boot on incoherent concurrency config | §P2. Oversubscription-with-a-strict-backstop is the documented, correct pattern; a boot gate would break correct deployments. Report loudly, never gate. |
| R11 | `parent_job_id` / chain linkage column | §P7. Migration + a write per enqueue to catch a class P1 catches by status. `originating_actor` is already promised and should land first. |
| R12 | Converge queue caps on boot with a drift log | Tempting symmetry with actor caps, but queue caps materialise pre-allocated `reservation_slots` rows and lowering one requires `sync_slots`, not `ensure_slots` (`_bootstrap.py:855-857`). Silent convergence would resize a strict fleet-wide cap as a side effect of a deploy. `taskq doctor` reporting staleness (P2) is the safe half. |
| R13 | Group `@actor`'s 21 keywords into an options object | Genuinely unclaimed (no prior art in docs or CHANGELOG) and genuinely the aggregate usability problem — but it is a large additive-with-deprecation surface change that prevents none of the 20 measured defects. Worth raising separately; not worth spending this budget on. |

---

## 4. Corrections — claims in the brief that the source disproves

Listed most to least consequential. Each cost me a reversal during this review.

1. **`advertised_retry_after` is not a TaskQ concept.** Zero matches in `src/`, `docs/`, `tests/`.
   It is a consumer-authored `RetryClassifierHook` in TAStack
   (`backend/packages/ta-worker/src/ta_worker/retry.py:67`).

2. **"`advertised_retry_after` overrode the DELAY but not the KIND" — the library always supported
   the kind.** `RetryOverride` carries both fields (`retry.py:209-210`), and its docstring names
   HTTP 429 → `indefinite` as the motivating example (`retry.py:196-201`). The *consumer's* first
   version omitted `kind`; the current version sets it. This was a consumer bug against a
   sufficient API, and it is already fixed.

3. **"The classifier fails a job once `attempt >= max_attempts` regardless" — false as stated.**
   The check is guarded by `effective_kind == "transient"` (`retry.py:302-303`). For `indefinite`
   it is never evaluated (`retry.py:312-318`).

4. **A budget-free reschedule already exists, twice.** `Snooze` and
   `RetryAfter(delay, consume_budget=False)` bypass classification entirely
   (`_handlers.py:783`, `:800`), implemented as `max_attempts + 1`
   (`_sql_templates.py:329`, `:454`). Both are documented, `RetryAfter` with the rate-limit case as its
   worked example (`retries.md:461-470`). Caveat worth knowing: the non-consuming path has **no**
   `max_attempts_failed` arm, so `schedule_to_close` is its only backstop.

5. **"Four concurrency layers" — the fourth is not what the brief says, and there is a fifth.**
   Workgroup per-child concurrency is not a distinct mechanism: it passes `--max-concurrency` to
   the child (`workgroup.py:136`), i.e. it *is* the process layer, configured per child. That is
   precisely why it was the invisible binding layer in the measured incident. `ops.md` §3 counts
   five by including `singleton=True`.

6. **"`ta_worker.schema.sync_actor_capacities` converges them on every boot" is not TaskQ
   behaviour.** TaskQ's `sync_actor_config` is **create-only** for all three capacity fields —
   they are deliberately absent from the `DO UPDATE SET` clause
   (`_UPSERT_ACTOR_CONFIG_SQL`, `src/taskq/worker/startup.py:37-46`, with the rationale at
   `:25-35`). No `converge`/`reconcile` function exists in `src/`. Boot-time
   convergence is a documented *user-side* pattern (`ops.md` §3, "Two viable ownership
   postures"); TAStack implements it.

7. **"A tier cap needs an explicit operator command AND a worker restart" — half right, and the
   asymmetry is the opposite axis from the brief's.** Neither actor nor queue caps converge in
   storage. The real split is *runtime readback*: actor `max_concurrent` is re-read **every
   dispatch cycle** and is live with no restart (`actor_config_ops.py:10-12`; the CTE joins
   `actor_config` at `_dispatch_sql.py:92`); `max_pending` is live within ~5 s via
   `ActorCapacityCache` (`_capacity.py:85`); only **queue** caps are read once at startup
   (`_bootstrap.py:876-882`) and need a restart (`cli.py:1625-1631`).

8. **A `config diff` command already exists.** `taskq actor-config diff --actors <module:attr>`
   (`cli.py:961-968`) prints literal, stored, and **effective** per actor per capacity field with
   its source (`_effective_capacity`, `cli.py:884-898`), flags structural drift, no-row actors, and
   leftover rows. Its docstring names the exact use case (`cli.py:972-981`). **There is no
   equivalent for queue caps** — that gap is real and P2 targets it.

9. **"Nothing in the API expresses a role" — correct, and nothing records lineage either.** No
   `parent_job_id`, no chain id, no depth counter. `batches.originating_actor` exists but is
   always `None` (`jobs-clients.md:818`).

10. **`open_running_run` does not exist in TaskQ, and neither does any "run" concept.** No `runs`
    table, no `run_id` column anywhere in `src/`. Mistake 6 is consumer-side and not fixable by a
    TaskQ change — though TaskQ has the same ambiguous-`None` defect in `get_batch`
    (`_batch_sql.py:285-290`) and `JobsClient.get` (`_jobs.py:1093-1096`). Credit where due:
    `wait_for_batch` already solved it properly with `on_empty="error"` and `EmptyBatchError`
    (`batch.py:393`, `exceptions.py:637`).

11. **A missing `actor_config` row means the actor never dispatches at all** — not "uncapped". The
    dispatch CTE draws candidates `FROM actor_config` (`_dispatch_sql.py:92`). Sharper than an
    over-admitting cap and worth its own alarm.

12. **Queue caps are enforced in the consumer, after the claim.** A denial snoozes `running →
    scheduled` (`_consumer.py:273-274`, `_handlers.py:527-536`), so a tight queue cap produces
    snooze churn rather than jobs waiting in `pending`. The *actor body* is strictly capped; the
    *claim rate* is not.

13. **The dedup retention horizon is 30 days by default**, not a short window —
    `DEFAULT_PRUNE_RETENTION = timedelta(days=30)` (`constants.py:134`), and `ops.md` §5's
    retention note gives 30 d succeeded / 90 d failed. This makes the status-blind collapse materially worse than
    the brief implies.

14. **`enqueue_batch` does not isolate per-item failures** (relevant to mistake 9): the primary
    path binds every item into one `unnest` INSERT in one transaction
    (`_enqueuer.py:316-318`). Only the autonomous fallback loops per item and raises
    `PartialBatchError` (`_enqueuer.py:376-404`), whose docstring documents that items before the
    first failure are already committed (`exceptions.py:449-455`).

15. **Minor, but it matters for mistake 2:** a cooperative cancel writes **no `error_class`**
    (`_sql_templates.py:290-300`), so operator-cancel and self-cancel are indistinguishable on the
    job row; and there is **no cancel hook** — only `ctx.cancellation_requested` polling
    (`context.py:72-91`). A job cancelled while `pending` never enters the handler at all
    (`_cancel_bulk.py:77-87`), which is exactly why an `updated_at`-versioned stage key cannot
    move.

---

## 5. If only one thing ships

**P1.** It is ~30 lines, purely additive, introduces no new failure mode, reuses two existing
mechanisms, and converts the highest-frequency silent failure in this deployment — a chain link or
a re-post vanishing into a terminal predecessor for up to 30 days — into a warning that names the
job, the status, and the fix. Mistakes 1 and 2 both funnel into it.
